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
	"google.golang.org/grpc"
)

const (
	sequencerBatchFrequencyMs = 100
)

func NewSequencer(raftID uint64, txnBatchChan chan<- *pb.TransactionBatch, peers []raft.Peer, storeDir string, connCache util.ConnectionCache, srvr *grpc.Server, logger *log.Entry) *sequencer {
	proposeChan := make(chan []byte)
	proposeConfChangeChan := make(chan raftpb.ConfChange)
	writerChan := make(chan *pb.Transaction)
	s := &sequencer{
		proposeChan:           proposeChan,
		proposeConfChangeChan: proposeConfChangeChan,
		writerChan:            writerChan,
		rb:                    newRaftBackend(raftID, proposeChan, proposeConfChangeChan, txnBatchChan, peers, storeDir, connCache, logger),
		logger:                logger,
	}

	pb.RegisterRaftTransportServer(srvr, s)
	go s.serveTxnBatches()
	return s
}

type sequencer struct {
	rb                    *raftBackend
	proposeChan           chan<- []byte
	proposeConfChangeChan chan<- raftpb.ConfChange
	writerChan            chan *pb.Transaction
	logger                *log.Entry
}

// transactions and distributed snapshot reads go here
func (s *sequencer) serveTxnBatches() {
	batch := &pb.TransactionBatch{}
	batchTicker := time.NewTicker(sequencerBatchFrequencyMs * time.Millisecond)
	defer batchTicker.Stop()

	for {
		select {
		case txn := <-s.writerChan:
			if txn == nil {
				s.logger.Warningf("Stop serving txn batches")
				close(s.proposeChan)
				close(s.proposeConfChangeChan)
				return
			}

			batch.Transactions = append(batch.Transactions, txn)

		case <-batchTicker.C:
			bites, err := batch.Marshal()
			if err != nil {
				s.logger.Panicf("%s", err)
			}

			s.proposeChan <- bites
			batch = &pb.TransactionBatch{}

		}
	}
}

func (s *sequencer) SubmitTransaction(txn *pb.Transaction) {
	s.writerChan <- txn
}

func (s *sequencer) Close() {
	close(s.writerChan)
}

func (s *sequencer) Step(ctx context.Context, req *pb.StepRequest) (*pb.StepResponse, error) {
	err := s.rb.step(ctx, *req.Message)
	return &pb.StepResponse{}, err
}
