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
	"math/rand"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/raftpb"
)

func TestBoltStorageBasic(t *testing.T) {
	dir := "./test-TestBoltStorageBasic-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	hardState, confState, err := store.InitialState()
	assert.Nil(t, err)
	assert.NotNil(t, hardState)
	assert.NotNil(t, confState)

	store.close()
}

func TestBoltStorageSaveSnapshot(t *testing.T) {
	dir := "./test-TestBoltStorageSaveSnapshot-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.NotNil(t, snap)

	snapToWrite := &raftpb.Snapshot{}
	snapToWrite.Metadata = raftpb.SnapshotMetadata{
		Index: uint64(99),
		Term:  uint64(77),
		ConfState: raftpb.ConfState{
			Nodes:    make([]uint64, 3),
			Learners: make([]uint64, 5),
		},
	}
	snapToWrite.Data = make([]byte, 33)
	err = store.saveSnap(*snapToWrite)
	assert.Nil(t, err)

	snap2, err := store.Snapshot()
	assert.Nil(t, err)
	assert.NotNil(t, snap2)
	assert.Equal(t, snapToWrite.Metadata.Index, snap2.Metadata.Index)
	assert.Equal(t, snapToWrite.Metadata.Term, snap2.Metadata.Term)

	store.close()
}

func TestBoltStorageFunWithEntries(t *testing.T) {
	dir := "./test-TestBoltStorageFunWithEntries-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	count := 97
	entsToStore := make([]raftpb.Entry, count)
	for i := 0; i < count; i++ {
		bites := make([]byte, i)
		rand.Read(bites)
		entsToStore[i] = raftpb.Entry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  raftpb.EntryNormal,
			Data:  bites,
		}
	}

	st := raftpb.HardState{
		Term:   uint64(101),
		Vote:   uint64(4),
		Commit: uint64(97),
	}

	err = store.saveEntriesAndState(entsToStore, st)
	assert.Nil(t, err)

	first, err := store.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), first)

	last, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(count-1), last)

	term, err := store.Term(uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, uint64(37), term)

	ents, err := store.Entries(uint64(37), uint64(37), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ents))

	ents, err = store.Entries(uint64(37), uint64(38), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(57))
	assert.Nil(t, err)
	// still only one because the byte size limitation kicks in
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(97))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)
	assert.Equal(t, uint64(38), ents[1].Index)

	// byte size is an exact match
	ents, err = store.Entries(uint64(37), uint64(40), uint64(138))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)
	assert.Equal(t, uint64(38), ents[1].Index)
	assert.Equal(t, uint64(39), ents[2].Index)

	store.close()
}

func TestBoltStorageNoKeysFound(t *testing.T) {
	dir := "./test-TestBoltStorageNoKeysFound-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	first, err := store.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), first)
	last, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), last)

	store.close()
}

func TestBoltStorageExistingDB(t *testing.T) {
	dir := "./test-TestBoltStorageExistingDB-" + uint64ToString(randomRaftId()) + "/"

	b := storageExists(dir)
	assert.False(t, b)

	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	b = storageExists(dir)
	assert.True(t, b)

	store.close()
}

func TestDropOldSnapshots(t *testing.T) {
	var err error
	dir := "./test-TestDropOldSnapshots-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	numSnapshotsToCreate := 47
	snapshotIndexes := make([]int, numSnapshotsToCreate)
	for i := 0; i < numSnapshotsToCreate; i++ {
		if i <= 0 {
			snapshotIndexes[i] = rand.Intn(1234)
		} else {
			snapshotIndexes[i] = rand.Intn(1234) + snapshotIndexes[i-1]
		}

		snap := makeRandomSnapshot(snapshotIndexes[i])
		err = store.saveSnap(snap)
		assert.Nil(t, err)
	}

	numItems, err := numItemsInBucket(store, snapshotsBucket)
	assert.Nil(t, err)
	assert.Equal(t, numSnapshotsToCreate, numItems)

	err = store.dropOldSnapshots(37)
	assert.Nil(t, err)
	numItems, err = numItemsInBucket(store, snapshotsBucket)
	assert.Nil(t, err)
	assert.Equal(t, 37, numItems)

	err = store.dropOldSnapshots(11)
	assert.Nil(t, err)
	numItems, err = numItemsInBucket(store, snapshotsBucket)
	assert.Nil(t, err)
	assert.Equal(t, 11, numItems)

	store.close()
}

func TestDropOldLogEntries(t *testing.T) {
	var err error
	dir := "./test-TestDropOldLogEntries-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	numEntriesToCreate := 1111
	startIndex := 123
	a := make([]raftpb.Entry, numEntriesToCreate-startIndex)
	hardState := raftpb.HardState{}
	for i := startIndex; i < numEntriesToCreate; i++ {
		e := makeRandomEntry(i)
		a[i-startIndex] = e
	}

	err = store.saveEntriesAndState(a, hardState)
	assert.Nil(t, err)

	numItems, err := numItemsInBucket(store, entriesBucket)
	assert.Nil(t, err)
	assert.Equal(t, numEntriesToCreate-startIndex, numItems)
	assert.True(t, areEntriesContiuous(store))

	dropIndex := uint64((numEntriesToCreate - startIndex) / 2)
	err = store.dropLogEntriesBeforeIndex(dropIndex)
	assert.Nil(t, err)
	numItems, err = numItemsInBucket(store, entriesBucket)
	assert.Nil(t, err)
	assert.Equal(t, uint64(numEntriesToCreate)-dropIndex, uint64(numItems))
	assert.True(t, areEntriesContiuous(store))

	entries, err := store.Entries(dropIndex, dropIndex+uint64(1), 1024*1024)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, uint64(494), entries[0].Index)

	entries, err = store.Entries(dropIndex-uint64(1), dropIndex, 1024*1024)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(entries))

	store.close()
}

func makeRandomSnapshot(index int) raftpb.Snapshot {
	metadata := raftpb.SnapshotMetadata{
		Index: uint64(index),
	}

	// make the snap grow over time
	bites := make([]byte, index)
	rand.Read(bites)

	return raftpb.Snapshot{
		Metadata: metadata,
		Data:     bites,
	}
}

func makeRandomEntry(index int) raftpb.Entry {
	// make the snap grow over time
	bites := make([]byte, index)
	rand.Read(bites)

	return raftpb.Entry{
		Index: uint64(index),
		Type:  raftpb.EntryNormal,
		Data:  bites,
	}
}

func numItemsInBucket(bs *boltStorage, bucketName []byte) (int, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return -1, err
	}

	defer tx.Rollback()
	// the stats might not be transactionally consistent but usually good enough
	return tx.Bucket(bucketName).Stats().KeyN, nil
}

func areEntriesContiuous(bs *boltStorage) bool {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return false
	}

	defer tx.Rollback()
	c := tx.Bucket(entriesBucket).Cursor()
	var key []byte
	var prevKey []byte
	prevKey, _ = c.First()
	for key != nil {
		key, _ = c.Next()
		if bytesToUint64(prevKey)+uint64(1) != bytesToUint64(key) {
			return false
		}
		prevKey = key
	}

	return true
}
