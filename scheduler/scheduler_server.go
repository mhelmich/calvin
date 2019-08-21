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
	"context"
	"sync"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	log "github.com/sirupsen/logrus"
)

func newServer(sequencerChan chan<- *pb.TransactionBatch, lowIsolationReads *sync.Map, logger *log.Entry) *schedulerServer {
	return &schedulerServer{
		sequencerChan:     sequencerChan,
		lowIsolationReads: lowIsolationReads,
		logger:            logger,
	}
}

type schedulerServer struct {
	sequencerChan     chan<- *pb.TransactionBatch
	lowIsolationReads *sync.Map
	logger            *log.Entry
}

// low isolation reads are tricky because they require communication between the execution package and the scheduler
// and they overload the transaction object with a completely different shape and purpose
// however the idea is:
// * a request for a low isolation read comes in here
// * a custom-build transaction batch is being put together
// * a response channel is set up in the shared map
// * the txn is sent to the sequencer channel and is treated like any other txn wrt locking and scheduling
// * the execution package knows how to identify and treat a low isolation read txn (the main part is: do the read and attach a LowIsolationReadResponse)
// * the txn is sent back via the done channel like any other txn
// * the scheduler looks for the low iso flag, finds the response channel, sends on it, and is done
func (ss *schedulerServer) LowIsolationRead(ctx context.Context, req *pb.LowIsolationReadRequest) (*pb.LowIsolationReadResponse, error) {
	id, _ := ulid.NewId()
	txnID := id.String()

	txn := &pb.Transaction{
		Id:                 id.ToProto(),
		ReadSet:            req.Keys,
		IsLowIsolationRead: true,
	}

	c := make(chan *pb.LowIsolationReadResponse, 1)
	ss.lowIsolationReads.Store(txnID, c)

	ss.sequencerChan <- &pb.TransactionBatch{
		Transactions: []*pb.Transaction{txn},
	}

	resp := <-c
	return resp, nil
}
