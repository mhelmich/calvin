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

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
)

func TestSequencerBasic(t *testing.T) {
	raftID := uint64(1)
	txnBatchChan := make(chan *pb.TransactionBatch)
	peers := []raft.Peer{raft.Peer{
		ID:      raftID,
		Context: []byte("narf"),
	}}
	storeDir := "./test-TestSequencerBasic-" + util.Uint64ToString(util.RandomRaftId()) + "/"
	defer os.RemoveAll(storeDir)
	mockCC := new(mocks.ConnectionCache)
	mockCIP := new(mocks.ClusterInfoProvider)
	srvr := grpc.NewServer()
	logger := log.WithFields(log.Fields{})

	s := NewSequencer(raftID, txnBatchChan, peers, storeDir, mockCC, mockCIP, srvr, logger)
	id, err := ulid.NewId()
	assert.Nil(t, err)

	s.SubmitTransaction(&pb.Transaction{
		Id: id.ToProto(),
	})
	sequencedTxnBatch := <-txnBatchChan
	assert.Equal(t, 1, len(sequencedTxnBatch.Transactions))
	sequencedTxnID, err := ulid.ParseIdFromProto(sequencedTxnBatch.Transactions[0].Id)
	assert.Nil(t, err)
	assert.Equal(t, id.String(), sequencedTxnID.String())
	s.Stop()
}
