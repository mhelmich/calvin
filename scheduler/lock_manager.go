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
	"bytes"
	"hash/fnv"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
)

type lockMode int

const (
	write lockMode = iota
	read
)

func newLockManager() *lockManager {
	return &lockManager{
		lockMap: make(map[uint64][]lockRequest),
	}
}

type lockRequest struct {
	txn  *pb.Transaction
	mode lockMode
	key  []byte
}

// The lockManager's lock map tracks all lock requests. For a
// given key, if 'lockMap' contains a nonempty array, then the item with
// that key is locked and either:
//  (a) first element in the array specifies the owner if that item is a
//      request for a write lock, or
//  (b) a read lock is held by all elements of the longest prefix of the array
//      containing only read lock requests.
type lockManager struct {
	lockMap map[uint64][]lockRequest
}

func (lm *lockManager) hash(key []byte) uint64 {
	hasher := fnv.New64()
	hasher.Write(key)
	return hasher.Sum64()
}

func (lm *lockManager) isKeyLocal(key []byte) bool {
	return true
}

func (lm *lockManager) lock(txn *pb.Transaction) {
	// lock the read-write set first
	// lock locals
	// double-check whether remote locks have been requested
	// if not, request them
	lm.innerLock(txn, write, txn.ReadWriteSet)

	// lock read set
	// lock locals
	// double-check whether remote locks have been requested
	// if not, request them
	lm.innerLock(txn, read, txn.ReadSet)
}

func (lm *lockManager) innerLock(txn *pb.Transaction, mode lockMode, set [][]byte) {
	for i := 0; i < len(set); i++ {
		key := set[i]
		if lm.isKeyLocal(key) {
			keyHash := lm.hash(key)
			lockRequests, ok := lm.lockMap[keyHash]
			if ok {
				j := 0
				for ; !bytes.Equal(key, lockRequests[j].key) && j < len(lockRequests); j++ {
					// hash collision
				}

				if j >= len(lockRequests) {
					// hash collision
					req := lockRequest{
						mode: mode,
						txn:  txn,
						key:  key,
					}
					lockRequests = append(lockRequests, req)
					lm.lockMap[keyHash] = lockRequests
					return
				}

				txnID, _ := ulid.ParseIdFromProto(txn.Id)
				for ; j < len(lockRequests); j++ {
					id, _ := ulid.ParseIdFromProto(lockRequests[j].txn.Id)
					if txnID.CompareTo(id) == 0 {
						// it seems I requested the lock already
						return
					}
				}

				if j >= len(lockRequests) {
					// I didn't request this lock yet, so adding my request
					req := lockRequest{
						mode: mode,
						txn:  txn,
						key:  key,
					}
					lockRequests = append(lockRequests, req)
					lm.lockMap[keyHash] = lockRequests
				}

			} else {
				// no entry in lock request map for this key
				// I will be the first one ... yay
				req := lockRequest{
					mode: mode,
					txn:  txn,
					key:  key,
				}
				lockRequests = append(lockRequests, req)
				lm.lockMap[keyHash] = lockRequests
			}
		}
	}
}

func (lm *lockManager) release(txn *pb.Transaction) {
	// find lock that was held and release it
	lm.innerRelease(txn.Id, txn.ReadWriteSet)
	lm.innerRelease(txn.Id, txn.ReadSet)

	// subsequent transactions might be able to run now...check that as well
}

func (lm *lockManager) innerRelease(txnIDProto *pb.Id128, set [][]byte) {
	for i := 0; i < len(set); i++ {
		key := set[i]
		if lm.isKeyLocal(key) {
			keyHash := lm.hash(key)
			lockRequests, ok := lm.lockMap[keyHash]

			if ok {
				txnID, _ := ulid.ParseIdFromProto(txnIDProto)
				i := 0
				for ; i < len(lockRequests); i++ {
					id, _ := ulid.ParseIdFromProto(lockRequests[i].txn.Id)
					if txnID.CompareTo(id) == 0 {
						break
					}
				}

				if i < len(lockRequests) {
					// remove i
					lockRequests = lm.removeIdx(lockRequests, i)
					if len(lockRequests) > 0 {
						lm.lockMap[keyHash] = lockRequests
					} else {
						delete(lm.lockMap, keyHash)
					}
				}
			}
		}
	}
}

func (lm *lockManager) removeIdx(lockRequests []lockRequest, idx int) []lockRequest {
	if idx == 0 {
		return lockRequests[1:]
	} else if idx == len(lockRequests)-1 {
		return lockRequests[:len(lockRequests)-1]
	}

	return append(lockRequests[:idx], lockRequests[idx+1:]...)
}
