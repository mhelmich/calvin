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

// think about adding a flavor using badger
// https://github.com/dgraph-io/badger

package calvin

import (
	"io"

	badger "github.com/dgraph-io/badger"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

func newBadgerDataStore(dir string, logger *log.Entry) *badgerDataStore {
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(logger))
	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	return &badgerDataStore{
		db:     db,
		logger: logger,
	}
}

type badgerDataStore struct {
	db     *badger.DB
	logger *log.Entry
}

func (bds *badgerDataStore) Snapshot(w io.Writer) error {
	err := bds.db.RunValueLogGC(0.5)
	if err != nil {
		return err
	}

	_, err = bds.db.Backup(w, uint64(0))
	return err
}

func (bds *badgerDataStore) StartTxn(writable bool) (util.DataStoreTxn, error) {
	txn := bds.db.NewTransaction(writable)
	return &badgerDataStoreTxn{
		txn:    txn,
		logger: bds.logger,
	}, nil
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
