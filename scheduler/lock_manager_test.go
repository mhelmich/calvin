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

package scheduler

import (
	"testing"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/stretchr/testify/assert"
)

func TestLockManagerBasic(t *testing.T) {
	lm := newLockManager()
	key := []byte("narf")
	keyHash := lm.hash(key)
	txnID, err := ulid.NewId()
	assert.Nil(t, err)

	readWriteSet := make([][]byte, 1)
	readWriteSet[0] = key
	txn := &pb.Transaction{
		Id:           txnID.ToProto(),
		ReadWriteSet: readWriteSet,
	}
	lm.lock(txn)

	requests, ok := lm.lockMap[keyHash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	receivedTxnID, err := ulid.ParseIdFromProto(requests[0].txn.Id)
	assert.Nil(t, err)
	assert.Equal(t, 0, txnID.CompareTo(receivedTxnID))

	lm.release(txn)
	requests, ok = lm.lockMap[keyHash]
	assert.False(t, ok)
	assert.Equal(t, 0, len(requests))
}
