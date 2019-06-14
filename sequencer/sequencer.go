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
		ElectionTick:    10,
		HeartbeatTick:   3,
		Storage:         bs,
		MaxSizePerMsg:   1024 * 1024 * 1024, // 1 GB (!!!)
		MaxInflightMsgs: 256,
		Logger:          logger,
	}

	n, err := raft.NewRawNode(c, nil)
	if err != nil {
		return nil, nil, err
	}

	s := &Sequencer{
		writerChan: writerChan,
		readerChan: readerChan,
		raftNode:   n,
		logger:     logger,
	}

	go s.runReader()
	go s.runWriter()
	return writerChan, readerChan, nil
}

type Sequencer struct {
	writerChan <-chan *pb.Transaction
	readerChan <-chan *pb.Transaction
	raftNode   *raft.RawNode
	logger     *log.Entry
}

// transactions and distributed snapshot reads go here
func (s *Sequencer) runWriter() {
	batch := &pb.TransactionBatch{}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	s.logger.Infof("Starting writer loop")

	for {
		select {
		case txn := <-s.writerChan:
			if txn == nil {
				s.logger.Infof("Ending writer loop")
				return
			}
			batch.Transactions = append(batch.Transactions, txn)
		case <-ticker.C:
			s.raftNode.Tick()
			s.logger.Infof("Tick")
			bites, err := batch.Marshal()
			if err != nil {
				s.logger.Errorf("%s", err)
			}

			err = s.raftNode.Propose(bites)
			if err != nil {
				s.logger.Errorf("%s", err)
			}

			batch = &pb.TransactionBatch{}
		}
	}
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
