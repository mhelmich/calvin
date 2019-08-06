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

type DataStore interface {
	Get(key []byte) []byte
	Set(key []byte, value []byte)
}

type LuaDataStore struct {
	ds DataStore
}

func (lds *LuaDataStore) Get(key string) string {
	return string(lds.ds.Get([]byte(key)))
}

func (lds *LuaDataStore) Set(key string, value string) {
	lds.ds.Set([]byte(key), []byte(value))
}
