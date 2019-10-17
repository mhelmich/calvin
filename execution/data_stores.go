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

package execution

import (
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

func newStoredProcDataStore(partitionedStore util.PartitionedDataStore, keys [][]byte, values [][]byte, cip util.ClusterInfoProvider) *storedProcDataStore {
	m := make(map[string][]byte, len(keys))

	for idx := range keys {
		k := string(keys[idx])
		v := values[idx]
		m[k] = v
	}

	return &storedProcDataStore{
		partitionedStore: partitionedStore,
		txns:             make(map[int]util.DataStoreTxn),
		data:             m,
		cip:              cip,
	}
}

type storedProcDataStore struct {
	partitionedStore util.PartitionedDataStore
	txns             map[int]util.DataStoreTxn
	data             map[string][]byte
	cip              util.ClusterInfoProvider
}

func (lds *storedProcDataStore) Get(key string) string {
	val, ok := lds.data[key]
	if !ok {
		log.Panicf("you tried to access key [%s] but wasn't in the keys declared to be accessed", key)
	} else if val == nil {
		return ""
	}
	return string(val)
}

func (lds *storedProcDataStore) Set(key string, value string) {
	if !lds.cip.IsLocal([]byte(key)) {
		// log.Warningf("you tried to set key [%s] but the key wasn't local", key)
		return
	}

	_, ok := lds.data[key]
	if !ok {
		log.Panicf("you tried to access key [%s] but wasn't in the keys declared to be accessed", key)
	}

	txn, err := lds.getTxnForKey([]byte(key))
	if err != nil {
		log.Panicf("can't get txn for key [%s]: %s", key, err.Error())
	}

	txn.Set([]byte(key), []byte(value))
}

func (lds *storedProcDataStore) getTxnForKey(key []byte) (util.DataStoreTxn, error) {
	partitionID := lds.cip.FindPartitionForKey(key)
	txn, ok := lds.txns[partitionID]
	if !ok {
		var txnProvider util.DataStoreTxnProvider
		var err error

		txnProvider, err = lds.partitionedStore.GetPartition(partitionID)
		if err != nil {
			return nil, err
		}

		txn, err = txnProvider.StartTxn(true)
		if err != nil {
			return nil, err
		}

		lds.txns[partitionID] = txn
	}
	return txn, nil
}

func (lds *storedProcDataStore) commit() error {
	defer func() { lds.txns = nil }()
	for _, txn := range lds.txns {
		err := txn.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (lds *storedProcDataStore) rollback() error {
	defer func() { lds.txns = nil }()
	for _, txn := range lds.txns {
		err := txn.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}
