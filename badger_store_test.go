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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBadgerStoreCreatingAndDeleting(t *testing.T) {
	baseDir := "./test-TestBadgerStoreCreatingAndDeleting-" + util.Uint64ToString(util.RandomRaftId()) + "/"
	err := os.MkdirAll(baseDir, os.ModePerm)
	assert.Nil(t, err)
	logger := log.WithFields(log.Fields{})
	bsp := newBadgerStoreProvider(baseDir, logger)
	m := &sync.Map{}
	count := 33

	for i := 0; i < count; i++ {
		store, err := bsp.CreatePartition(i)
		assert.Nil(t, err)
		m.Store(i, store)
		exists, err := pathExists(fmt.Sprintf("%spartition-%d", baseDir, i))
		assert.Nil(t, err)
		assert.True(t, exists)
	}

	m.Range(func(key, value interface{}) bool {
		p := value.(util.DataStoreTxnProvider)
		p.Delete()
		return true
	})

	time.Sleep(100 * time.Millisecond)
	for i := 0; i < count; i++ {
		exists, err := pathExists(fmt.Sprintf("%spartition-%d", baseDir, i))
		assert.Nil(t, err)
		assert.False(t, exists)
	}

	err = os.RemoveAll(baseDir)
	assert.Nil(t, err)
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
