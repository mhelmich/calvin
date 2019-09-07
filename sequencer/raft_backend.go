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
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/jsonpb"
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
		ElectionTick:    11,
		HeartbeatTick:   3,
		Storage:         bs,
		MaxSizePerMsg:   1024 * 1024 * 1024, // 1 GB (!!!)
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
		raftID:                  raftID,
		raftNode:                raftNode,
		proposeChan:             proposeChan,
		proposeConfChangeChan:   proposeConfChangeChan,
		txnBatchChan:            txnBatchChan,
		connCache:               connCache,
		store:                   bs,
		confState:               &raftpb.ConfState{},
		snapshotFrequency:       1000,
		numberOfSnapshotsToKeep: 2,
		logger:                  logger,
	}

	go rb.runRaftStateMachine()
	go rb.serveProposalChannels()
	return rb
}

type raftBackend struct {
	raftID                  uint64
	raftNode                raft.Node
	proposeChan             <-chan []byte
	proposeConfChangeChan   <-chan raftpb.ConfChange
	txnBatchChan            chan<- *pb.TransactionBatch
	store                   *boltStorage
	lastAppliedIndex        uint64 // The last index that has been applied. It helps us figuring out which entries to publish.
	lastSnapshotIndex       uint64 // The index of the last snapshot
	snapshotFrequency       uint64
	numberOfSnapshotsToKeep int
	confState               *raftpb.ConfState
	connCache               util.ConnectionCache
	startChan               chan interface{}
	snapshotHandler         SnapshotHandler
	logger                  *log.Entry
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
	if !raft.IsEmptySnap(rd.Snapshot) {
		rb.publishSnapshot(rd.Snapshot)
	}

	rb.broadcastMessages(rd.Messages)
	rb.publishEntries(rb.entriesToApply(rd.CommittedEntries))
	rb.maybeTriggerSnapshot()
	rb.raftNode.Advance()
}

func (rb *raftBackend) broadcastMessages(msgs []raftpb.Message) {
	// spawn a go routine for every recipient
	// every go rountine is seeded with a particular recipientID
	// and will only send messages for this recipient to this recipient
	// this only works if msgs is not modified
	wg := &sync.WaitGroup{}
	peers := make(map[uint64]bool)
	for idx := range msgs {
		_, ok := peers[msgs[idx].To]
		if !ok {
			peers[msgs[idx].To] = true
			wg.Add(1)
			go rb.innerBroadcastMessages(msgs[idx].To, idx, msgs, wg)
		}
	}

	wg.Wait()
}

func (rb *raftBackend) innerBroadcastMessages(recipientID uint64, startIdx int, msgs []raftpb.Message, wg *sync.WaitGroup) {
	defer wg.Done()

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

	indexesToMsgsSent := []int{}

	for idx := startIdx; idx < len(msgs); idx++ {
		if msgs[idx].To == recipientID {
			msg := msgs[idx]
			req := &pb.StepRequest{
				Message: &msg,
			}

			err := stream.Send(req)
			if err != nil {
				rb.raftNode.ReportUnreachable(recipientID)
				if msg.Type == raftpb.MsgSnap {
					rb.raftNode.ReportSnapshot(recipientID, raft.SnapshotFailure)
				}
				rb.logger.Errorf("%s", err.Error())
			}

			indexesToMsgsSent = append(indexesToMsgsSent, idx)
		}
	}

	err = stream.CloseSend()
	if err != nil {
		rb.raftNode.ReportUnreachable(recipientID)
		rb.logger.Errorf("%s", err.Error())
		return
	}

	for idx := 0; idx < len(indexesToMsgsSent); idx++ {
		resp, err := stream.Recv()
		if err != nil {
			rb.raftNode.ReportUnreachable(recipientID)
			rb.logger.Errorf("%s", err.Error())
			if msgs[indexesToMsgsSent[idx]].Type == raftpb.MsgSnap {
				rb.raftNode.ReportSnapshot(recipientID, raft.SnapshotFailure)
			}
		} else if resp.Error != "" {
			rb.raftNode.ReportUnreachable(recipientID)
			rb.logger.Errorf("%s", resp.Error)
			if msgs[indexesToMsgsSent[idx]].Type == raftpb.MsgSnap {
				rb.raftNode.ReportSnapshot(recipientID, raft.SnapshotFailure)
			}
		}

		if msgs[indexesToMsgsSent[idx]].Type == raftpb.MsgSnap {
			rb.raftNode.ReportSnapshot(recipientID, raft.SnapshotFinish)
		}
	}
}

func (rb *raftBackend) entriesToApply(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return []raftpb.Entry{}
	}

	firstIdx := ents[0].Index
	if firstIdx > rb.lastAppliedIndex+1 {
		// if I'm getting invalid data, I'm shutting down
		rb.logger.Panicf("First index of committed entry [%d] should <= progress.lastAppliedIndex[%d] !", firstIdx, rb.lastAppliedIndex)
		return []raftpb.Entry{}
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

		rb.lastAppliedIndex = ents[idx].Index
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

func (rb *raftBackend) publishSnapshot(snap raftpb.Snapshot) {
	if rb.snapshotHandler == nil {
		return
	}

	// call consumer first
	err := rb.snapshotHandler.Consume(snap.Data)
	if err != nil {
		rb.logger.Errorf("Snapshot handler consume failed: %s", err.Error())
		return
	}

	// store the snapshot on disk
	err = rb.store.saveSnap(snap)
	if err != nil {
		rb.logger.Errorf("Couldn't persist snapshot: %s", err.Error())
		return
	}

	rb.confState = &snap.Metadata.ConfState
	rb.lastSnapshotIndex = snap.Metadata.Index
	rb.lastAppliedIndex = snap.Metadata.Index
}

func (rb *raftBackend) maybeTriggerSnapshot() {
	if rb.lastAppliedIndex-rb.lastSnapshotIndex < rb.snapshotFrequency || rb.snapshotHandler == nil {
		// we didn't collect enough entries yet to warrant a new snapshot
		return
	}

	lastSnapshot, err := rb.store.Snapshot()
	if err != nil {
		rb.logger.Errorf("Couldn't get the last snapshot")
		return
	}

	entriesSinceLastSnapshot, err := rb.store.Entries(lastSnapshot.Metadata.Index, rb.lastAppliedIndex+1, 1024*1024*1024)
	if err != nil {
		rb.logger.Errorf("Couldn't get entries since last snapshot")
		return
	}

	// get snapshot from data structure
	data, err := rb.snapshotHandler.Provide(lastSnapshot, entriesSinceLastSnapshot)
	if err != nil {
		rb.logger.Errorf("Snapshot handler provide failed: %s", err.Error())
		return
	}

	// bake snapshot object by tossing all the metadata and byte arrays in there
	snap, err := rb.bakeNewSnapshot(data)
	if err != nil {
		rb.logger.Errorf("Can't bake new snapshot: %s", err.Error())
		return
	}

	// save the snapshot
	err = rb.store.saveSnap(snap)
	if err != nil {
		rb.logger.Errorf("Can't save new snapshot: %s", err.Error())
		return
	}

	// drop all log entries before the snapshot index
	err = rb.store.dropLogEntriesBeforeIndex(snap.Metadata.Index)
	if err != nil {
		rb.logger.Errorf("Couldn't delete old log entries: %s", err.Error())
	}

	// drop old snapshots if applicable
	err = rb.store.dropOldSnapshots(rb.numberOfSnapshotsToKeep)
	if err != nil {
		rb.logger.Errorf("Couldn't delete old snaphots: %s", err.Error())
	}
}

func (rb *raftBackend) bakeNewSnapshot(data []byte) (raftpb.Snapshot, error) {
	hardState, confState, err := rb.store.InitialState()
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	metadata := raftpb.SnapshotMetadata{
		ConfState: confState,
		Index:     rb.lastAppliedIndex,
		Term:      hardState.Term,
	}

	return raftpb.Snapshot{
		Data:     data,
		Metadata: metadata,
	}, nil
}

func (rb *raftBackend) step(ctx context.Context, msg raftpb.Message) error {
	return rb.raftNode.Step(ctx, msg)
}

func (rb *raftBackend) logToJSON(out io.Writer, n int) error {
	hi, err := rb.store.LastIndex()
	if err != nil {
		return err
	}

	var lo uint64
	if uint64(n) > hi {
		lo = uint64(0)
	} else {
		lo = hi - uint64(n+1)
	}

	entries, err := rb.store.Entries(lo, hi+1, uint64(1024*1024*1024))
	if err != nil {
		return err
	}

	rb.logger.Warningf("dumping %d raft log entries onto the wire [%d] [%d]", len(entries), hi, lo)

	jpb := &jsonpb.Marshaler{Indent: "  "}
	out.Write([]byte("{"))
	for i := 0; i < len(entries); i++ {
		jpb.Marshal(out, &entries[i])
		if i < len(entries)-1 {
			out.Write([]byte(",\n"))
		}
	}
	out.Write([]byte("}"))

	return nil
}
