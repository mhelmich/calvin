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
	"encoding/hex"
	"time"

	"github.com/mhelmich/calvin/pb"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	sequencerBatchFrequencyMs = 10
)

func NewSequencer() (chan<- *pb.Transaction, chan<- *pb.Transaction, error) {
	writerChan := make(chan *pb.Transaction)
	readerChan := make(chan *pb.Transaction)

	newRaftId := randomRaftId()
	logger := log.WithFields(log.Fields{
		"component": "sequencer",
		"raftIdHex": hex.EncodeToString(uint64ToBytes(newRaftId)),
		"raftId":    uint64ToString(newRaftId),
	})

	storeDir := "./" + "raft-" + uint64ToString(newRaftId) + "/"
	// startFromExistingState := storageExists(storeDir)
	bs, err := openBoltStorage(storeDir, logger)
	if err != nil {
		logger.Errorf("Can't open data store: %s", err.Error())
		return nil, nil, err
	}

	c := &raft.Config{
		ID:              newRaftId,
		ElectionTick:    5,
		HeartbeatTick:   3,
		Storage:         bs,
		MaxSizePerMsg:   1024 * 1024 * 1024, // 1 GB (!!!)
		MaxInflightMsgs: 256,
		Logger:          logger,
	}

	raftPeers := make([]raft.Peer, 1)
	raftPeers[0] = raft.Peer{
		ID:      newRaftId,
		Context: []byte("narf"),
	}

	n, err := raft.NewRawNode(c, raftPeers)
	if err != nil {
		return nil, nil, err
	}

	s := &Sequencer{
		raftId:     newRaftId,
		writerChan: writerChan,
		readerChan: readerChan,
		raftNode:   n,
		store:      bs,
		logger:     logger,
	}

	go s.runReader()
	go s.runWriter()
	return writerChan, readerChan, nil
}

type Sequencer struct {
	raftId     uint64
	writerChan <-chan *pb.Transaction
	readerChan <-chan *pb.Transaction
	raftNode   *raft.RawNode
	store      store // the raft data store
	transport  raftTransport
	logger     *log.Entry
}

// transactions and distributed snapshot reads go here
func (s *Sequencer) runWriter() {
	batch := &pb.TransactionBatch{}
	raftTicker := time.NewTicker(10 * time.Millisecond)
	batchTicker := time.NewTicker(sequencerBatchFrequencyMs * time.Millisecond)
	defer raftTicker.Stop()
	defer batchTicker.Stop()

	s.logger.Infof("Starting writer loop")

	for {
		select {
		case txn := <-s.writerChan:
			if txn == nil {
				s.logger.Infof("Ending writer loop")
				return
			}
			batch.Transactions = append(batch.Transactions, txn)

		case <-raftTicker.C:
			s.raftNode.Tick()

		case <-batchTicker.C:
			bites, err := batch.Marshal()
			if err != nil {
				s.logger.Errorf("%s", err)
			}

			err = s.raftNode.Propose(bites)
			if err != nil {
				s.logger.Errorf("%s", err)
			}

			batch = &pb.TransactionBatch{}

		default:
			if s.raftNode.HasReady() {
				s.logger.Infof("Processing ready...")
				rd := s.raftNode.Ready()
				s.processReady(rd)
				s.raftNode.Advance(rd)
			} else {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func (s *Sequencer) processReady(rd raft.Ready) {
	s.logger.Debugf("ID: %d %x Hardstate: %v Entries: %v Snapshot: %v Messages: %v Committed: %v", s.raftId, s.raftId, rd.HardState, rd.Entries, rd.Snapshot, rd.Messages, rd.CommittedEntries)
	s.store.saveEntriesAndState(rd.Entries, rd.HardState)

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := s.store.saveSnap(rd.Snapshot); err != nil {
			s.logger.Errorf("Couldn't save snapshot: %s", err.Error())
			return
		}

		s.publishSnapshot(rd.Snapshot)
	}

	sendingErrors := s.transport.sendMessages(rd.Messages)
	if sendingErrors != nil {
		for _, failedMsg := range sendingErrors.failedMessages {
			// TODO - think this through
			// rb.logger.Errorf("Reporting raft [%d %x] unreachable", failedMsg.To, failedMsg.To)
			// rb.raftNode.ReportUnreachable(failedMsg.To)
			if isMsgSnap(failedMsg) {
				s.logger.Errorf("Reporting snapshot failure for raft [%d %x]", failedMsg.To, failedMsg.To)
				s.raftNode.ReportSnapshot(failedMsg.To, raft.SnapshotFailure)
			}
		}

		for _, snapMsg := range sendingErrors.succeededSnapshotMessages {
			s.raftNode.ReportSnapshot(snapMsg.To, raft.SnapshotFinish)
		}
	}
}

func (s *Sequencer) publishSnapshot(snap raftpb.Snapshot) {
}

// low-isolation reads and single partition snapshot reads go here
func (s *Sequencer) runReader() {
	for {
		select {
		case txn := <-s.readerChan:
			if txn == nil {
				return
			}
		}
	}
}
