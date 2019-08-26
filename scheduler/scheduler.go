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
	"io"
	"sync"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Scheduler struct {
	sequencerChan     <-chan *pb.TransactionBatch
	readyTxnsChan     chan<- *pb.Transaction
	doneTxnChan       <-chan *pb.Transaction
	lockMgr           *lockManager
	lowIsolationReads *sync.Map
	logger            *log.Entry
}

func NewScheduler(sequencerChan chan *pb.TransactionBatch, readyTxnsChan chan<- *pb.Transaction, doneTxnChan <-chan *pb.Transaction, srvr *grpc.Server, logger *log.Entry) *Scheduler {
	lowIsolationReads := &sync.Map{}
	s := &Scheduler{
		sequencerChan:     sequencerChan,
		readyTxnsChan:     readyTxnsChan,
		doneTxnChan:       doneTxnChan,
		lockMgr:           newLockManager(),
		lowIsolationReads: lowIsolationReads,
		logger:            logger,
	}

	ss := newServer(sequencerChan, lowIsolationReads, logger)
	pb.RegisterLowIsolationReadServer(srvr, ss)

	go s.runLocker()
	go s.runReleaser()
	return s
}

func (s *Scheduler) runLocker() {
	for {
		batch, ok := <-s.sequencerChan
		if !ok {
			close(s.readyTxnsChan)
			s.logger.Warningf("Stopping lock locker")
			return
		} else if batch == nil {
			s.logger.Warningf("Received nil txn batch")
		}

		for idx := range batch.Transactions {
			txn := batch.Transactions[idx]
			if log.GetLevel() == log.DebugLevel {
				id, _ := ulid.ParseIdFromProto(txn.Id)
				s.logger.Debugf("getting locks for txn [%s]", id.String())
			}

			numLocksNotAcquired := s.lockMgr.lock(txn)

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

func (s *Scheduler) runReleaser() {
	for {
		txn, ok := <-s.doneTxnChan
		if !ok {
			s.logger.Warningf("Stopping lock releaser")
			return
		}

		if log.GetLevel() == log.DebugLevel {
			id, _ := ulid.ParseIdFromProto(txn.Id)
			s.logger.Debugf("txn [%s] became done\n", id.String())
		}

		// in addition to the regular stuff, low iso reads need
		// the response out of the txn object to be sent on the response channel
		if txn.IsLowIsolationRead {
			id, _ := ulid.ParseIdFromProto(txn.Id)
			txnID := id.String()
			v, ok := s.lowIsolationReads.Load(txnID)
			if !ok {
				s.logger.Panicf("can't find low isolation read channel for txn [%s]", txnID)
			}
			c := v.(chan *pb.LowIsolationReadResponse)
			c <- txn.LowIsolationReadResponse
			close(c)
			s.lowIsolationReads.Delete(txnID)
		}

		newOwners := s.lockMgr.release(txn)

		for idx := range newOwners {
			if log.GetLevel() == log.DebugLevel {
				id, _ := ulid.ParseIdFromProto(newOwners[idx].Id)
				s.logger.Debugf("txn [%s] became ready\n", id.String())
			}
			s.readyTxnsChan <- newOwners[idx]
		}
	}
}

func (s *Scheduler) LockChainToAscii(out io.Writer) {
	s.lockMgr.lockChainToAscii(out)
}
