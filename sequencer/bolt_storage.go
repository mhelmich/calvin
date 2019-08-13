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
	"bytes"
	"os"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/gogo/protobuf/proto"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type localRaftStore interface {
	raft.Storage
	saveConfigState(confState raftpb.ConfState) error
	saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error
	dropLogEntriesBeforeIndex(index uint64) error
	saveSnap(snap raftpb.Snapshot) error
	dropOldSnapshots(numberOfSnapshotsToKeep int) error
	close()
}

const (
	databaseName = "raft.db"
)

var (
	staticFieldsBucket = []byte("static")
	entriesBucket      = []byte("entries")
	hardStateKey       = []byte("hardstate")
	confStateKey       = []byte("conf")
	snapshotsBucket    = []byte("snapshots")
)

type boltStorage struct {
	db     *bolt.DB
	logger *log.Entry
}

func storageExists(dir string) bool {
	if _, err := os.Stat(dir + databaseName); os.IsNotExist(err) {
		return false
	} else if err != nil {
		return false
	}

	return true
}

func openBoltStorage(dir string, parentLogger *log.Entry) (*boltStorage, error) {
	var err error
	if !storageExists(dir) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(dir+databaseName, 0600, &bolt.Options{Timeout: 1 * time.Second, NoFreelistSync: true})
	if err != nil {
		return nil, err
	}

	err = initBoltStorage(db)
	if err != nil {
		return nil, err
	}

	if parentLogger == nil {
		parentLogger = log.WithFields(log.Fields{
			"component": "storage",
		})
	} else {
		parentLogger = parentLogger.WithFields(log.Fields{
			"component": "storage",
		})
	}

	return &boltStorage{
		db:     db,
		logger: parentLogger,
	}, nil
}

func initBoltStorage(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		var err error

		if _, err = tx.CreateBucketIfNotExists([]byte(staticFieldsBucket)); err != nil {
			return err
		}

		if _, err = tx.CreateBucketIfNotExists([]byte(snapshotsBucket)); err != nil {
			return err
		}

		if _, err = tx.CreateBucketIfNotExists([]byte(entriesBucket)); err != nil {
			return err
		}

		return nil
	})
}

// InitialState returns the saved HardState and ConfState information.
func (bs *boltStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, raft.ErrUnavailable
	}

	defer tx.Rollback()
	st, stErr := bs.loadHardState(tx)
	if stErr != nil {
		bs.logger.Errorf("Can't load hard state: %s", stErr.Error())
	}

	cSt, cStErr := bs.loadConfState(tx)
	if cStErr != nil {
		bs.logger.Errorf("Can't load hard state: %s", cStErr.Error())
	}

	var stToReturn raftpb.HardState
	var cStToReturn raftpb.ConfState
	var errToReturn error

	if stErr == nil && st != nil {
		stToReturn = *st
		errToReturn = nil
	} else {
		stToReturn = raftpb.HardState{}
		errToReturn = stErr
	}

	if cStErr == nil && cSt != nil {
		cStToReturn = *cSt
	} else {
		cStToReturn = raftpb.ConfState{}
		errToReturn = cStErr
	}

	bs.logger.Infof("Starting node with: %s %s", stToReturn.String(), cStToReturn.String())
	return stToReturn, cStToReturn, errToReturn
}

func (bs *boltStorage) loadHardState(tx *bolt.Tx) (*raftpb.HardState, error) {
	st := &raftpb.HardState{}
	bites := tx.Bucket(staticFieldsBucket).Get(hardStateKey)
	if bites == nil || len(bites) == 0 {
		// return st, raft.ErrUnavailable
		return st, nil
	}

	err := proto.Unmarshal(bites, st)
	if err != nil {
		return st, err
	}

	return st, nil
}

func (bs *boltStorage) loadConfState(tx *bolt.Tx) (*raftpb.ConfState, error) {
	confSt := &raftpb.ConfState{}
	bites := tx.Bucket(staticFieldsBucket).Get(confStateKey)
	if bites == nil || len(bites) == 0 {
		// return confSt, raft.ErrUnavailable
		return confSt, nil
	}

	err := proto.Unmarshal(bites, confSt)
	if err != nil {
		return confSt, err
	}

	return confSt, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (bs *boltStorage) Entries(lo, hi, maxByteSize uint64) ([]raftpb.Entry, error) {
	min := util.Uint64ToBytes(lo)
	max := util.Uint64ToBytes(hi)
	entries := make([]raftpb.Entry, hi-lo)

	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	c := tx.Bucket(entriesBucket).Cursor()

	idx := 0
	runningByteSize := uint64(0)
	// loop over all keys excluding (!!!) the high key
	for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) < 0; k, v = c.Next() {
		// we need to honor the max byte size parameter
		// but in any case at least return one (if one is present)
		if uint64(len(v))+runningByteSize > maxByteSize && idx > 0 {
			break
		}

		// add byte size to running counter
		runningByteSize += uint64(len(v))

		// go about my business of deserializing the entry
		e := &raftpb.Entry{}
		err = proto.Unmarshal(v, e)
		if err != nil {
			bs.logger.Errorf("Can't unmarshal: %s", err.Error())
			return nil, err
		}

		entries[idx] = *e
		idx++
	}

	return entries[:idx], nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (bs *boltStorage) Term(i uint64) (uint64, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	bites := tx.Bucket(entriesBucket).Get(util.Uint64ToBytes(i))
	if bites == nil || len(bites) == 0 {
		// return 0, raft.ErrUnavailable
		return 0, nil
	}

	entry := &raftpb.Entry{}
	err = proto.Unmarshal(bites, entry)
	if err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (bs *boltStorage) LastIndex() (uint64, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	curs := tx.Bucket(entriesBucket).Cursor()
	last, _ := curs.Last()
	if last == nil {
		// if we can't find a key in the bucket,
		// return 0 as last index
		return 0, nil
	}

	return util.BytesToUint64(last), nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (bs *boltStorage) FirstIndex() (uint64, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		bs.logger.Errorf("Failed getting first index: %s", err.Error())
		return 0, err
	}

	defer tx.Rollback()
	curs := tx.Bucket(entriesBucket).Cursor()
	first, _ := curs.First()
	if first == nil {
		// if we can't find a key in the bucket,
		// return 1 as frist index
		return 1, nil
	}

	return util.BytesToUint64(first), nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (bs *boltStorage) Snapshot() (raftpb.Snapshot, error) {
	snap := &raftpb.Snapshot{}
	err := bs.db.View(func(tx *bolt.Tx) error {
		_, bites := tx.Bucket(snapshotsBucket).Cursor().Last()

		if bites != nil && len(bites) > 0 {
			return proto.Unmarshal(bites, snap)
		}

		return nil
	})

	return *snap, err
}

// The order in which entries and hardState are dealt with is actually important.
// Raft dictates that first the entries need to be written to disk and then
// states are supposed to be handled. Even though the store we use supports
// transactions and atomic writes...
func (bs *boltStorage) saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error {
	tx1, err := bs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx1.Rollback()
	b := tx1.Bucket(entriesBucket)
	for _, e := range entries {
		bites, err2 := proto.Marshal(&e)
		if err2 != nil {
			return err2
		}

		err2 = b.Put(util.Uint64ToBytes(e.Index), bites)
		if err2 != nil {
			return err2
		}
	}

	if len(entries) > 0 {
		lastEntryIWrote := entries[len(entries)-1]
		lastIndexIWrote := util.Uint64ToBytes(lastEntryIWrote.Index)
		c := tx1.Bucket(entriesBucket).Cursor()
		k, _ := c.Last()
		for ; k != nil && bytes.Compare(k, lastIndexIWrote) > 0; k, _ = c.Prev() {
			err2 := c.Delete()
			if err2 != nil {
				return err2
			}
		}
	}

	err = tx1.Commit()
	if err != nil {
		return err
	}

	if !raft.IsEmptyHardState(hardState) {
		tx2, err := bs.db.Begin(true)
		if err != nil {
			return err
		}

		defer tx2.Rollback()
		bites, err := proto.Marshal(&hardState)
		if err != nil {
			return err
		}

		err = tx2.Bucket(staticFieldsBucket).Put(hardStateKey, bites)
		if err != nil {
			return err
		}

		err = tx2.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// deletes all log entries older than the index provided
func (bs *boltStorage) dropLogEntriesBeforeIndex(index uint64) error {
	var key []byte
	var err error

	tx, err := bs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	c := tx.Bucket(entriesBucket).Cursor()
	// find index
	c.Seek(util.Uint64ToBytes(index))
	// move to the previous key
	// and start deleteing
	key, _ = c.Prev()

	for key != nil {
		err = c.Delete()
		if err != nil {
			bs.logger.Errorf("Couldn't delete key: %d", util.BytesToUint64(key))
		}
		key, _ = c.Prev()
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Saves a raft snapshot by its index.
// An existing with the same index will be overridden.
func (bs *boltStorage) saveSnap(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	tx, err := bs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	bites, err := proto.Marshal(&snap)
	if err != nil {
		return err
	}

	err = tx.Bucket(snapshotsBucket).Put(util.Uint64ToBytes(snap.Metadata.Index), bites)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Drops all existing indexes and only keeps the latest snapshots.
func (bs *boltStorage) dropOldSnapshots(numberOfSnapshotsToKeep int) error {
	var k []byte
	tx, err := bs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	c := tx.Bucket(snapshotsBucket).Cursor()
	c.Last()
	for i := 0; i < numberOfSnapshotsToKeep; i++ {
		k, _ = c.Prev()
	}

	for k != nil {
		err = c.Delete()
		if err != nil {
			bs.logger.Errorf("Can't delete key %d: %s", util.BytesToUint64(k), err.Error())
		}

		k, _ = c.Prev()
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (bs *boltStorage) saveConfigState(confState raftpb.ConfState) error {
	tx, err := bs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	bites, err := proto.Marshal(&confState)
	if err != nil {
		return err
	}

	err = tx.Bucket(staticFieldsBucket).Put(confStateKey, bites)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (bs *boltStorage) close() {
	bs.db.Close()
}
