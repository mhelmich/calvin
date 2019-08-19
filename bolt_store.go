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
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

const (
	bucketName = "default"
	dbName     = "data.db"
)

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
		logger: logger,
	}
}

type boltDataStore struct {
	db     *bolt.DB
	logger *log.Entry
}

func (bds *boltDataStore) StartTxn(writable bool) (util.DataStoreTxn, error) {
	txn, err := bds.db.Begin(writable)
	t := &boltDataStoreTxn{
		txn: txn,
	}
	return t, err
}

type boltDataStoreTxn struct {
	txn *bolt.Tx
}

func (t *boltDataStoreTxn) Get(key []byte) []byte {
	b := t.txn.Bucket([]byte(bucketName))
	return b.Get(key)
}

func (t *boltDataStoreTxn) Set(key []byte, value []byte) error {
	b := t.txn.Bucket([]byte(bucketName))
	return b.Put(key, value)
}

func (t *boltDataStoreTxn) Commit() error {
	return t.txn.Commit()
}

func (t *boltDataStoreTxn) Rollback() error {
	return t.txn.Rollback()
}

func (bds *boltDataStore) close() {
	bds.db.Close()
}
