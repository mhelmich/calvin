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

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger"
	calvinpb "github.com/mhelmich/calvin/pb"
	calvinutil "github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

func newPartitionedBadgerStore(baseDir string, logger *log.Entry) *partitionedBadgerStore {
	if !strings.HasSuffix(baseDir, "/") {
		baseDir = baseDir + "/"
	}
	return &partitionedBadgerStore{
		baseDir:    baseDir,
		partitions: &sync.Map{},
		logger:     logger,
	}
}

type partitionedBadgerStore struct {
	baseDir    string
	partitions *sync.Map
	logger     *log.Entry
}

func (bdsp *partitionedBadgerStore) CreatePartition(partitionID int) (calvinutil.DataStoreTxnProvider, error) {
	dir := fmt.Sprintf("%spartition-%d", bdsp.baseDir, partitionID)
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(bdsp.logger))
	if err != nil {
		return nil, err
	}

	bds := &badgerDataStore{
		db:          db,
		dir:         dir,
		partitionID: partitionID,
		logger:      bdsp.logger,
	}

	v, loaded := bdsp.partitions.LoadOrStore(partitionID, bds)
	if loaded {
		bds.Close()
		bds = v.(*badgerDataStore)
	}
	return bds, nil
}

func (bdsp *partitionedBadgerStore) Snapshot(w io.Writer) error {
	var err error
	ps := &calvinpb.PartitionedSnapshot{}
	bdsp.partitions.Range(func(key, value interface{}) bool {
		partitionID := key.(uint64)
		bds := value.(*badgerDataStore)
		buf := new(bytes.Buffer)
		err = bds.Snapshot(buf)
		if err != nil {
			return false
		}

		ps.PartitionIDs = append(ps.PartitionIDs, partitionID)
		ps.Snapshots = append(ps.Snapshots, buf.Bytes())
		return true
	})

	if err != nil {
		bdsp.logger.Errorf("can't collect partitioned snapshot: %s", err.Error())
		return err
	}

	data, err := ps.Marshal()
	if err != nil {
		bdsp.logger.Errorf("can't create partitioned snapshot: %s", err.Error())
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		bdsp.logger.Errorf("can't write partitioned snapshot: %s", err.Error())
		return err
	}

	return nil
}

func (bdsp *partitionedBadgerStore) Close() {
	bdsp.partitions.Range(func(key, value interface{}) bool {
		bds := value.(*badgerDataStore)
		defer bds.Close()
		return true
	})
}

func newBadgerDataStore(dir string, logger *log.Entry) *badgerDataStore {
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(logger))
	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	return &badgerDataStore{
		db:          db,
		dir:         dir,
		partitionID: 0,
		logger:      logger,
	}
}

type badgerDataStore struct {
	db          *badger.DB
	dir         string
	partitionID int
	logger      *log.Entry
}

func (bds *badgerDataStore) Snapshot(w io.Writer) error {
	err := bds.db.RunValueLogGC(0.8)
	if err != nil && err != badger.ErrNoRewrite {
		return err
	}

	ts, err := bds.db.Backup(w, 0)
	bds.logger.Infof("Created backup on partition %d for ts %d", bds.partitionID, ts)
	return err
}

func (bds *badgerDataStore) StartTxn(writable bool) (calvinutil.DataStoreTxn, error) {
	txn := bds.db.NewTransaction(writable)
	return &badgerDataStoreTxn{
		txn:    txn,
		logger: bds.logger,
	}, nil
}

func (bds *badgerDataStore) Delete() {
	defer func() {
		bds.Close()
		os.RemoveAll(bds.dir)
	}()
}

func (bds *badgerDataStore) Close() {
	bds.db.Close()
}

type badgerDataStoreTxn struct {
	txn    *badger.Txn
	logger *log.Entry
}

func (t *badgerDataStoreTxn) Get(key []byte) []byte {
	i, err := t.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		t.logger.Panicf("%s", err.Error())
		return nil
	}

	bites, err := i.ValueCopy(nil)
	if err != nil {
		t.logger.Panicf("%s", err.Error())
		return nil
	}
	return bites
}

func (t *badgerDataStoreTxn) Set(key []byte, value []byte) error {
	return t.txn.Set(key, value)
}

func (t *badgerDataStoreTxn) Commit() error {
	return t.txn.Commit()
}

func (t *badgerDataStoreTxn) Rollback() error {
	t.txn.Discard()
	return nil
}
