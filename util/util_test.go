/*
 * Copyright 2018 Marco Helmich
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

package util

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConversions(t *testing.T) {
	i := RandomRaftId()

	bites := Uint64ToBytes(i)
	assert.Equal(t, i, BytesToUint64(bites))

	str := Uint64ToString(i)
	assert.Equal(t, i, stringToUint64(str))
}

func sizeOfMap(m *sync.Map) int {
	var i int
	m.Range(func(key, value interface{}) bool {
		i++
		return true
	})

	return i
}

func removeAll(dir string) error {
	defer func() {
		time.Sleep(100 * time.Millisecond)
		os.RemoveAll(dir)
	}()
	return nil
}
