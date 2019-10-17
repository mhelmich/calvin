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

package calvin

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	bolt "github.com/coreos/bbolt"
	calvinpb "github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

const (
	bucketName = "default"
	dbName     = "data.db"
)

func newPartitionedBoltStore(baseDir string, logger *log.Entry) *partitionedBoltStore {
	if !strings.HasSuffix(baseDir, "/") {
		baseDir = baseDir + "/"
	}
	return &partitionedBoltStore{
		baseDir:    baseDir,
		partitions: &sync.Map{},
		logger:     logger,
	}
}

type partitionedBoltStore struct {
	baseDir    string
	partitions *sync.Map
	logger     *log.Entry
}

func (pbs *partitionedBoltStore) CreatePartition(partitionID int) (util.DataStoreTxnProvider, error) {
	dir := fmt.Sprintf("%spartition-%d", pbs.baseDir, partitionID)
	db, err := bolt.Open(dir+dbName, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		if _, err = tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	bds := &boltDataStore{
		db:          db,
		dir:         dir,
		partitionID: partitionID,
		logger:      pbs.logger,
	}

	v, loaded := pbs.partitions.LoadOrStore(partitionID, bds)
	if loaded {
		bds.Close()
		bds = v.(*boltDataStore)
	}
	return bds, nil
}

func (pbs *partitionedBoltStore) GetPartition(partitionID int) (util.DataStoreTxnProvider, error) {
	v, ok := pbs.partitions.Load(partitionID)
	if !ok {
		return nil, fmt.Errorf("can't find partition with id [%d]", partitionID)
	}
	return v.(util.DataStoreTxnProvider), nil
}

func (pbs *partitionedBoltStore) Snapshot(w io.Writer) error {
	var err error
	ps := &calvinpb.PartitionedSnapshot{}
	pbs.partitions.Range(func(key, value interface{}) bool {
		partitionID := key.(uint64)
		bds := value.(*boltDataStore)
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
		pbs.logger.Errorf("can't collect partitioned snapshot: %s", err.Error())
		return err
	}

	data, err := ps.Marshal()
	if err != nil {
		pbs.logger.Errorf("can't create partitioned snapshot: %s", err.Error())
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		pbs.logger.Errorf("can't write partitioned snapshot: %s", err.Error())
		return err
	}

	return nil
}

func (pbs *partitionedBoltStore) Close() {
	pbs.partitions.Range(func(key, value interface{}) bool {
		bds := value.(*boltDataStore)
		defer bds.Close()
		return true
	})
}

func newBoltDataStore(dir string, logger *log.Entry) *boltDataStore {
	db, err := bolt.Open(dir+dbName, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		if _, err = tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	return &boltDataStore{
		db:     db,
		dir:    dir,
		logger: logger,
	}
}

type boltDataStore struct {
	db          *bolt.DB
	dir         string
	partitionID int
	logger      *log.Entry
}

func (bds *boltDataStore) StartTxn(writable bool) (util.DataStoreTxn, error) {
	txn, err := bds.db.Begin(writable)
	t := &boltDataStoreTxn{
		txn:    txn,
		bucket: txn.Bucket([]byte(bucketName)),
	}
	return t, err
}

func (bds *boltDataStore) Snapshot(w io.Writer) error {
	// TODO: implement this method
	return nil
}

func (bds *boltDataStore) Close() {
	bds.db.Close()
}

func (bds *boltDataStore) Delete() {
	defer func() {
		bds.Close()
		os.RemoveAll(bds.dir + dbName)
	}()
}

// Later you might wanna look at this:
// https://github.com/etcd-io/bbolt#batch-read-write-transactions
type boltDataStoreTxn struct {
	txn *bolt.Tx
	// need the bucket in here to prevent race conditions
	bucket *bolt.Bucket
}

func (t *boltDataStoreTxn) Get(key []byte) []byte {
	return t.bucket.Get(key)
}

func (t *boltDataStoreTxn) Set(key []byte, value []byte) error {
	err := t.bucket.Put(key, value)
	return err
}

func (t *boltDataStoreTxn) Commit() error {
	return t.txn.Commit()
}

func (t *boltDataStoreTxn) Rollback() error {
	return t.txn.Rollback()
}
