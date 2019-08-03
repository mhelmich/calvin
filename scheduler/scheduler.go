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
	"github.com/mhelmich/calvin/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type scheduler struct {
	sequencerChan <-chan *pb.TransactionBatch
	doneTxnChan   <-chan *pb.Transaction
	readyTxns     chan<- *pb.Transaction
	lockMgr       *lockManager

	logger *log.Entry
}

func NewScheduler(sequencerChan <-chan *pb.TransactionBatch, srvr *grpc.Server, logger *log.Entry) *scheduler {
	s := &scheduler{
		sequencerChan: sequencerChan,
		lockMgr:       newLockManager(),
		logger:        logger,
	}

	ss := newServer(logger)
	pb.RegisterSchedulerServer(srvr, ss)

	go s.runLockManager()
	return s
}

func (s *scheduler) runLockManager() {
	for {
		select {
		case batch := <-s.sequencerChan:
			if batch == nil {
				return
			}

			for idx := range batch.Transactions {
				txn := batch.Transactions[idx]
				if s.lockMgr.lock(txn) == 0 {
					s.readyTxns <- txn
				}
			}

		case txn := <-s.doneTxnChan:
			if txn == nil {
				return
			}

			newOwners := s.lockMgr.release(txn)
			for idx := range newOwners {
				s.readyTxns <- newOwners[idx].txn
			}

		}
	}
}
