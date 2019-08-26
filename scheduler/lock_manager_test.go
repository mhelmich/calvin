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
	"fmt"
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

	readWriteSet := [][]byte{key}
	txn := &pb.Transaction{
		Id:           txnID.ToProto(),
		ReadWriteSet: readWriteSet,
	}
	numLocksNotAcquired := lm.lock(txn)
	assert.Equal(t, 0, numLocksNotAcquired)

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

func TestLockManagerMultipleTxns(t *testing.T) {
	lm := newLockManager()
	key1 := []byte("key1")
	key1Hash := lm.hash(key1)
	key2 := []byte("key2")
	key2Hash := lm.hash(key2)

	txnID1, err := ulid.NewId()
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           txnID1.ToProto(),
		ReadWriteSet: [][]byte{key1},
		ReadSet:      [][]byte{key2},
	}

	txnID2, err := ulid.NewId()
	assert.Nil(t, err)
	txn2 := &pb.Transaction{
		Id:           txnID2.ToProto(),
		ReadWriteSet: [][]byte{key2},
		ReadSet:      [][]byte{key1},
	}

	numLocksNotAcquired := lm.lock(txn1)
	assert.Equal(t, 0, numLocksNotAcquired)
	requests, ok := lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	numLocksNotAcquired = lm.lock(txn2)
	assert.Equal(t, 2, numLocksNotAcquired)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))

	lm.release(txn2)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	lm.release(txn1)
	requests, ok = lm.lockMap[key2Hash]
	assert.False(t, ok)
	assert.Equal(t, 0, len(requests))
	requests, ok = lm.lockMap[key1Hash]
	assert.False(t, ok)
	assert.Equal(t, 0, len(requests))
}

func TestLockManagerSameTxnTwice(t *testing.T) {
	lm := newLockManager()
	key1 := []byte("key1")
	key1Hash := lm.hash(key1)
	key2 := []byte("key2")
	key2Hash := lm.hash(key2)

	txnID1, err := ulid.NewId()
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           txnID1.ToProto(),
		ReadWriteSet: [][]byte{key1},
		ReadSet:      [][]byte{key2},
	}

	lm.lock(txn1)
	requests, ok := lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	lm.lock(txn1)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
}

func TestLockManagerTxnsReleaseMiddle(t *testing.T) {
	lm := newLockManager()
	key1 := []byte("key1")
	key1Hash := lm.hash(key1)
	key2 := []byte("key2")
	key2Hash := lm.hash(key2)

	txnID1, err := ulid.NewId()
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           txnID1.ToProto(),
		ReadWriteSet: [][]byte{key1},
		ReadSet:      [][]byte{key2},
	}

	txnID2, err := ulid.NewId()
	assert.Nil(t, err)
	txn2 := &pb.Transaction{
		Id:           txnID2.ToProto(),
		ReadWriteSet: [][]byte{key2},
		ReadSet:      [][]byte{key1},
	}

	txnID3, err := ulid.NewId()
	assert.Nil(t, err)
	txn3 := &pb.Transaction{
		Id:           txnID3.ToProto(),
		ReadWriteSet: [][]byte{key2},
		ReadSet:      [][]byte{key1},
	}

	lm.lock(txn1)
	requests, ok := lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	lm.lock(txn2)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))

	lm.lock(txn3)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 3, len(requests))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 3, len(requests))

	lm.release(txn2)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))
	id1, _ := ulid.ParseIdFromProto(requests[0].txn.Id)
	assert.Equal(t, 0, txnID1.CompareTo(id1))
	id2, _ := ulid.ParseIdFromProto(requests[1].txn.Id)
	assert.Equal(t, 0, txnID3.CompareTo(id2))
	requests, ok = lm.lockMap[key2Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))
	id1, _ = ulid.ParseIdFromProto(requests[0].txn.Id)
	assert.Equal(t, 0, txnID1.CompareTo(id1))
	id2, _ = ulid.ParseIdFromProto(requests[1].txn.Id)
	assert.Equal(t, 0, txnID3.CompareTo(id2))
}

func TestLockManagerLockInheritanceWriteToWrite(t *testing.T) {
	lm := newLockManager()
	key1 := []byte("key1")
	key1Hash := lm.hash(key1)

	txnID1, err := ulid.NewId()
	fmt.Printf("txnID1: %s\n", txnID1.String())
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           txnID1.ToProto(),
		ReadWriteSet: [][]byte{key1},
	}

	txnID2, err := ulid.NewId()
	fmt.Printf("txnID2: %s\n", txnID2.String())
	assert.Nil(t, err)
	txn2 := &pb.Transaction{
		Id:           txnID2.ToProto(),
		ReadWriteSet: [][]byte{key1},
	}

	numLocksNotAcquired := lm.lock(txn1)
	assert.Equal(t, 0, numLocksNotAcquired)
	requests, ok := lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	numLocksNotAcquired = lm.lock(txn2)
	assert.Equal(t, 1, numLocksNotAcquired)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))

	lm.release(txn1)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	lm.release(txn2)
	requests, ok = lm.lockMap[key1Hash]
	assert.False(t, ok)
	assert.Equal(t, 0, len(requests))
}

func TestLockManagerLockInheritanceWriteToReads(t *testing.T) {
	lm := newLockManager()
	key1 := []byte("key1")
	key1Hash := lm.hash(key1)

	txnID1, err := ulid.NewId()
	fmt.Printf("txnID1: %s\n", txnID1.String())
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           txnID1.ToProto(),
		ReadWriteSet: [][]byte{key1},
	}

	txnID2, err := ulid.NewId()
	fmt.Printf("txnID2: %s\n", txnID2.String())
	assert.Nil(t, err)
	txn2 := &pb.Transaction{
		Id:      txnID2.ToProto(),
		ReadSet: [][]byte{key1},
	}

	txnID3, err := ulid.NewId()
	fmt.Printf("txnID3: %s\n", txnID3.String())
	assert.Nil(t, err)
	txn3 := &pb.Transaction{
		Id:      txnID3.ToProto(),
		ReadSet: [][]byte{key1},
	}

	numLocksNotAcquired := lm.lock(txn1)
	assert.Equal(t, 0, numLocksNotAcquired)
	requests, ok := lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests))

	numLocksNotAcquired = lm.lock(txn2)
	assert.Equal(t, 1, numLocksNotAcquired)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))

	numLocksNotAcquired = lm.lock(txn3)
	assert.Equal(t, 1, numLocksNotAcquired)
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 3, len(requests))

	newOwners := lm.release(txn1)
	assert.Equal(t, 2, len(newOwners))
	assert.True(t, txn2.Equal(newOwners[0]))
	assert.True(t, txn3.Equal(newOwners[1]))
	requests, ok = lm.lockMap[key1Hash]
	assert.True(t, ok)
	assert.Equal(t, 2, len(requests))
}
