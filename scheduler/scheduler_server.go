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
