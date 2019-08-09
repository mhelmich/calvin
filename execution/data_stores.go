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
	"bytes"

	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
)

type DataStore interface {
	Get(key []byte) []byte
	Set(key []byte, value []byte)
}

type luaDataStore struct {
	ds     DataStore
	keys   [][]byte
	values [][]byte
	cip    util.ClusterInfoProvider
}

func (lds *luaDataStore) Get(key string) string {
	val, ok := lds.getValueFor([]byte(key))
	if !ok {
		log.Panicf("you tried to access key [%s] but wasn't in the keys declared to be accessed", key)
	} else if val == nil {
		return string(lds.ds.Get([]byte(key)))
	}
	return string(val)
}

func (lds *luaDataStore) Set(key string, value string) {
	if !lds.cip.IsLocal([]byte(key)) {
		//log.Warningf("you tried to access key [%s] but the key wasn't local", key)
		return
	}

	_, ok := lds.getValueFor([]byte(key))
	if !ok {
		log.Panicf("you tried to access key [%s] but wasn't in the keys declared to be accessed", key)
	}

	lds.ds.Set([]byte(key), []byte(value))
}

func (lds *luaDataStore) getValueFor(key []byte) ([]byte, bool) {
	for idx := range lds.keys {
		if bytes.Compare(lds.keys[idx], key) == 0 {
			return lds.values[idx], true
		}
	}
	return nil, false
}
