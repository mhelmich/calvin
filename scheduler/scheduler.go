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

	go s.runLockManager()
	return s
}

func (s *Scheduler) runLockManager() {
	for {
		select {
		case batch := <-s.sequencerChan:
			if batch == nil {
				close(s.readyTxnsChan)
				return
			}

			for idx := range batch.Transactions {
				txn := batch.Transactions[idx]
				s.logger.Debugf("getting locks for txn [%s]", txn.Id.String())
				if s.lockMgr.lock(txn) == 0 {
					// readyId, _ := ulid.ParseIdFromProto(txn.Id)
					// fmt.Printf("txn [%s] became ready\n", readyId.String())
					s.logger.Debugf("txn [%s] became ready", txn.Id.String())
					s.readyTxnsChan <- txn
				}
			}

		case txn := <-s.doneTxnChan:
			if txn == nil {
				close(s.readyTxnsChan)
				return
			}

			// readyId, _ := ulid.ParseIdFromProto(txn.Id)
			// fmt.Printf("txn [%s] became done\n", readyId.String())
			s.logger.Debugf("txn [%s] became done", txn.Id.String())

			newOwners := s.lockMgr.release(txn)
			for idx := range newOwners {
				// readyId, _ := ulid.ParseIdFromProto(newOwners[idx].txn.Id)
				// fmt.Printf("txn [%s] became ready\n", readyId.String())
				s.logger.Debugf("txn [%s] became ready", newOwners[idx].txn.Id.String())
				s.readyTxnsChan <- newOwners[idx].txn
			}

		}
	}
}
