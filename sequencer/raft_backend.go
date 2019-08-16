/*
 * Copyright 2019 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sequencer

import (
	"context"
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

func newRaftBackend(raftID uint64, proposeChan <-chan []byte, proposeConfChangeChan <-chan raftpb.ConfChange, txnBatchChan chan<- *pb.TransactionBatch, peers []raft.Peer, storeDir string, connCache util.ConnectionCache, logger *log.Entry) *raftBackend {
	bs, err := openBoltStorage(storeDir, logger)
	if err != nil {
		logger.Panicf("%s", err.Error())
	}

	c := &raft.Config{
		ID:              raftID,
		ElectionTick:    7,
		HeartbeatTick:   5,
		Storage:         bs,
		MaxSizePerMsg:   1024 * 1024, // 1 MB (!!!)
		MaxInflightMsgs: 256,
		Logger:          logger,
	}

	startFromExistingState := false
	var raftNode raft.Node
	if startFromExistingState {
		hardState, _, _ := bs.InitialState()
		c.Applied = hardState.Commit
		raftNode = raft.RestartNode(c)
	} else {
		for idx := range peers {
			logger.Infof("raftID: %d", peers[idx].ID)
		}
		raftNode = raft.StartNode(c, peers)
	}

	rb := &raftBackend{
		raftID:                raftID,
		raftNode:              raftNode,
		proposeChan:           proposeChan,
		proposeConfChangeChan: proposeConfChangeChan,
		txnBatchChan:          txnBatchChan,
		connCache:             connCache,
		store:                 bs,
		confState:             &raftpb.ConfState{},
		logger:                logger,
	}

	go rb.runRaftStateMachine()
	go rb.serveProposalChannels()
	return rb
}

type raftBackend struct {
	raftID                uint64
	raftNode              raft.Node
	proposeChan           <-chan []byte
	proposeConfChangeChan <-chan raftpb.ConfChange
	txnBatchChan          chan<- *pb.TransactionBatch
	store                 *boltStorage
	appliedIndex          uint64 // The last index that has been applied. It helps us figuring out which entries to publish.
	confState             *raftpb.ConfState
	connCache             util.ConnectionCache
	startChan             chan interface{}
	logger                *log.Entry
}

func (rb *raftBackend) serveProposalChannels() {
	for {
		select {
		case prop := <-rb.proposeChan:
			if prop == nil {
				rb.raftNode.Stop()
				close(rb.txnBatchChan)
				return
			}

			// blocks until accepted by raft state machine
			err := rb.raftNode.Propose(context.Background(), prop)
			if err != nil {
				rb.logger.Panicf("%s\n", err.Error())
			}

		case cc, ok := <-rb.proposeConfChangeChan:
			if !ok {
				rb.raftNode.Stop()
				close(rb.txnBatchChan)
				return
			}

			// blocks until accepted by raft state machine
			err := rb.raftNode.ProposeConfChange(context.Background(), cc)
			if err != nil {
				rb.logger.Panicf("%s\n", err.Error())
			}
		}
	}
}

func (rb *raftBackend) runRaftStateMachine() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rb.raftNode.Tick()

		case rd, ok := <-rb.raftNode.Ready():
			if !ok {
				rb.raftNode.Stop()
				return
			}
			rb.processReady(rd)

		}
	}
}

func (rb *raftBackend) processReady(rd raft.Ready) {
	rb.store.saveEntriesAndState(rd.Entries, rd.HardState)
	rb.broadcastMessages(rd.Messages)
	rb.publishEntries(rb.entriesToApply(rd.CommittedEntries))
	rb.raftNode.Advance()
}

func (rb *raftBackend) broadcastMessages(msgs []raftpb.Message) {
	peers := make(map[uint64]bool)
	for idx := range msgs {
		_, ok := peers[msgs[idx].To]
		if !ok {
			peers[msgs[idx].To] = true
			go rb.innerBroadcastMessages(msgs[idx].To, idx, msgs)
		}
	}
}

func (rb *raftBackend) innerBroadcastMessages(recipientID uint64, startIdx int, msgs []raftpb.Message) {
	client, err := rb.connCache.GetRaftTransportClient(recipientID)
	if err != nil {
		rb.raftNode.ReportUnreachable(recipientID)
		rb.logger.Errorf("%s", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.StepStream(ctx)
	if err != nil {
		rb.raftNode.ReportUnreachable(recipientID)
		rb.logger.Errorf("%s", err.Error())
		return
	}

	numMsgsSent := 0

	for idx := startIdx; idx < len(msgs); idx++ {
		if msgs[idx].To == recipientID {
			msg := msgs[idx]
			req := &pb.StepRequest{
				Message: &msg,
			}

			err := stream.Send(req)
			if err != nil {
				rb.raftNode.ReportUnreachable(recipientID)
				rb.logger.Errorf("%s", err.Error())
			}
			numMsgsSent++
		}
	}

	err = stream.CloseSend()
	if err != nil {
		rb.raftNode.ReportUnreachable(recipientID)
		rb.logger.Errorf("%s", err.Error())
		return
	}

	for idx := 0; idx < numMsgsSent; idx++ {
		resp, err := stream.Recv()
		if err != nil {
			rb.raftNode.ReportUnreachable(recipientID)
			rb.logger.Errorf("%s", err.Error())
		} else if resp.Error != "" {
			rb.raftNode.ReportUnreachable(recipientID)
			rb.logger.Errorf("%s", resp.Error)
		}
	}
}

func (rb *raftBackend) entriesToApply(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return make([]raftpb.Entry, 0)
	}

	firstIdx := ents[0].Index
	if firstIdx > rb.appliedIndex+1 {
		// if I'm getting invalid data, I'm shutting down
		rb.logger.Panicf("First index of committed entry [%d] should <= progress.appliedIndex[%d] !", firstIdx, rb.appliedIndex)
		return make([]raftpb.Entry, 0)
	}

	return ents
}

func (rb *raftBackend) publishEntries(ents []raftpb.Entry) {
	for idx := range ents {
		switch ents[idx].Type {
		case raftpb.EntryNormal:
			rb.publishTransactionBatch(ents[idx])

		case raftpb.EntryConfChange:
			rb.publishConfigChange(ents[idx])
		}

		rb.appliedIndex = ents[idx].Index
	}
}

func (rb *raftBackend) publishTransactionBatch(entry raftpb.Entry) {
	if len(entry.Data) <= 0 {
		return
	}

	batch := &pb.TransactionBatch{}
	err := batch.Unmarshal(entry.Data)
	if err != nil {
		rb.logger.Panicf(err.Error())
	}

	rb.txnBatchChan <- batch
}

func (rb *raftBackend) publishConfigChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	cc.Unmarshal(entry.Data)
	rb.confState = rb.raftNode.ApplyConfChange(cc)
	rb.store.saveConfigState(*rb.confState)
}

func (rb *raftBackend) step(ctx context.Context, msg raftpb.Message) error {
	return rb.raftNode.Step(ctx, msg)
}
