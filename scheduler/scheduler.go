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

package scheduler

import (
	"sync"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Scheduler struct {
	sequencerChan <-chan *pb.TransactionBatch
	readyTxnsChan chan<- *pb.Transaction
	doneTxnChan   <-chan *pb.Transaction
	lockMgr       *lockManager
	logger        *log.Entry
}

func NewScheduler(sequencerChan <-chan *pb.TransactionBatch, readyTxnsChan chan<- *pb.Transaction, doneTxnChan <-chan *pb.Transaction, srvr *grpc.Server, logger *log.Entry) *Scheduler {
	s := &Scheduler{
		sequencerChan: sequencerChan,
		readyTxnsChan: readyTxnsChan,
		doneTxnChan:   doneTxnChan,
		lockMgr:       newLockManager(),
		logger:        logger,
	}

	ss := newServer(logger)
	pb.RegisterSchedulerServer(srvr, ss)

	m := &sync.Mutex{}
	go s.runLocker(m)
	go s.runReleaser(m)
	return s
}

func (s *Scheduler) runLocker(m *sync.Mutex) {
	for {
		batch := <-s.sequencerChan
		if batch == nil {
			close(s.readyTxnsChan)
			return
		}

		for idx := range batch.Transactions {
			txn := batch.Transactions[idx]
			if log.GetLevel() == log.DebugLevel {
				id, _ := ulid.ParseIdFromProto(txn.Id)
				s.logger.Debugf("getting locks for txn [%s]", id.String())
			}

			m.Lock()
			numLocksNotAcquired := s.lockMgr.lock(txn)
			m.Unlock()

			if numLocksNotAcquired == 0 {
				if log.GetLevel() == log.DebugLevel {
					id, _ := ulid.ParseIdFromProto(txn.Id)
					s.logger.Debugf("txn [%s] became ready\n", id.String())
				}
				s.readyTxnsChan <- txn
			}
		}
	}
}

func (s *Scheduler) runReleaser(m *sync.Mutex) {
	for {
		txn := <-s.doneTxnChan
		if txn == nil {
			close(s.readyTxnsChan)
			return
		}

		if log.GetLevel() == log.DebugLevel {
			id, _ := ulid.ParseIdFromProto(txn.Id)
			s.logger.Debugf("txn [%s] became done\n", id.String())
		}

		m.Lock()
		newOwners := s.lockMgr.release(txn)
		m.Unlock()

		for idx := range newOwners {
			if log.GetLevel() == log.DebugLevel {
				id, _ := ulid.ParseIdFromProto(newOwners[idx].txn.Id)
				s.logger.Debugf("txn [%s] became ready\n", id.String())
			}
			s.readyTxnsChan <- newOwners[idx].txn
		}
	}
}
