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
	"crypto/rand"
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
	mockSH := new(mocks.SnapshotHandler)
	logger := log.WithFields(log.Fields{})

	newRaftBackend(raftID, proposeChan, proposeConfChangeChan, txnBatchChan, peers, storeDir, mockCC, mockSH, logger)
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
	assert.Equal(t, 1, len(txnBatch.Transactions))
	receivedID, err := ulid.ParseIdFromProto(txnBatch.Transactions[0].Id)
	assert.Nil(t, err)
	assert.Equal(t, id.String(), receivedID.String())
	close(proposeChan)
	close(proposeConfChangeChan)
}

func TestRaftBackendTriggerSnapshotBasic(t *testing.T) {
	storeDir := "./test-TestRaftBackendTriggerSnapshotBasic-" + util.Uint64ToString(util.RandomRaftId()) + "/"
	store, err := openBoltStorage(storeDir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	rb := &raftBackend{
		lastAppliedIndex:  uint64(99),
		lastSnapshotIndex: uint64(98),
		snapshotFrequency: uint64(0),
		snapshotHandler:   &testSnapshotHandler{},
		store:             store,
	}

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), snap.Metadata.Index)

	rb.maybeTriggerSnapshot()

	snap, err = store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, rb.lastAppliedIndex, snap.Metadata.Index)
	assert.Equal(t, 2048, len(snap.Data))

	store.close()
	removeAll(storeDir)
}

func TestRaftBackendTriggerSnapshotExistingSnapAndEntries(t *testing.T) {
	storeDir := "./test-TestRaftBackendTriggerSnapshotExistingSnapAndEntries-" + util.Uint64ToString(util.RandomRaftId()) + "/"
	store, err := openBoltStorage(storeDir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	rb := &raftBackend{
		lastAppliedIndex:  uint64(99),
		lastSnapshotIndex: uint64(96),
		snapshotFrequency: uint64(2),
		snapshotHandler: &testSnapshotHandler{
			t:                           t,
			entriesThatShouldBeProvided: []uint64{97, 98, 99},
			indexOfSnapshot:             uint64(96),
		},
		store:                   store,
		numberOfSnapshotsToKeep: 1,
	}

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), snap.Metadata.Index)

	// fill in snapshot and entries
	metadata := raftpb.SnapshotMetadata{
		ConfState: raftpb.ConfState{},
		Index:     uint64(96),
		Term:      uint64(3),
	}

	oldSnapBites := make([]byte, 1024)
	rand.Read(oldSnapBites)
	oldSnap := raftpb.Snapshot{
		Metadata: metadata,
		Data:     oldSnapBites,
	}
	err = store.saveSnap(oldSnap)
	assert.Nil(t, err)

	entries := []raftpb.Entry{
		raftpb.Entry{
			Index: uint64(97),
			Term:  uint64(3),
		},
		raftpb.Entry{
			Index: uint64(98),
			Term:  uint64(3),
		},
		raftpb.Entry{
			Index: uint64(99),
			Term:  uint64(3),
		},
	}
	hardState := raftpb.HardState{}
	err = store.saveEntriesAndState(entries, hardState)
	assert.Nil(t, err)

	rb.maybeTriggerSnapshot()

	snap, err = store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, rb.lastAppliedIndex, snap.Metadata.Index)
	assert.Equal(t, 2048, len(snap.Data))

	store.close()
	removeAll(storeDir)
}

type testSnapshotHandler struct {
	t                           *testing.T
	entriesThatShouldBeProvided []uint64
	indexOfSnapshot             uint64
}

func (h *testSnapshotHandler) Consume(snapshotData []byte) error {
	return nil
}

func (h *testSnapshotHandler) Provide(lastSnapshot raftpb.Snapshot, entriesAppliedSinceLastSnapshot []raftpb.Entry) ([]byte, error) {
	if h.entriesThatShouldBeProvided != nil {
		for i := 0; i < len(h.entriesThatShouldBeProvided); i++ {
			assert.Equal(h.t, h.entriesThatShouldBeProvided[i], entriesAppliedSinceLastSnapshot[i].Index)
		}
	}

	if h.indexOfSnapshot > uint64(0) {
		assert.Equal(h.t, h.indexOfSnapshot, lastSnapshot.Metadata.Index)
	}

	bites := make([]byte, 2048)
	rand.Read(bites)
	return bites, nil
}

func removeAll(dir string) error {
	defer func() {
		time.Sleep(100 * time.Millisecond)
		os.RemoveAll(dir)
	}()
	return nil
}
