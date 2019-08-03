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
	"os"
	"testing"
	"time"

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

func TestRaftBackendBasic(t *testing.T) {
	raftID := uint64(1)
	proposeChan := make(chan []byte)
	proposeConfChangeChan := make(chan raftpb.ConfChange)
	txnBatchChan := make(chan *pb.TransactionBatch)
	peers := []raft.Peer{raft.Peer{
		ID:      raftID,
		Context: []byte("narf"),
	}}
	storeDir := "./test-TestRaftBackendBasic-" + util.Uint64ToString(util.RandomRaftId()) + "/"
	defer os.RemoveAll(storeDir)
	mockCC := new(mocks.ConnectionCache)
	logger := log.WithFields(log.Fields{})

	newRaftBackend(raftID, proposeChan, proposeConfChangeChan, txnBatchChan, peers, storeDir, mockCC, logger)
	time.Sleep(1 * time.Second)
	id, err := ulid.NewId()
	assert.Nil(t, err)
	batch := &pb.TransactionBatch{
		Transactions: []*pb.Transaction{&pb.Transaction{
			Id: id.ToProto(),
		}},
	}
	bites, err := batch.Marshal()
	assert.Nil(t, err)
	proposeChan <- bites
	txnBatch := <-txnBatchChan
	assert.NotNil(t, txnBatch)
}
