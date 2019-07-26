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

func (lm *lockManager) lock(txn *pb.Transaction) int {
	numLocksNotAcquired := 0
	// lock the read-write set first
	// lock locals
	// double-check whether remote locks have been requested
	// if not, request them
	numLocksNotAcquired += lm.innerLock(txn, write, txn.ReadWriteSet)

	// lock read set
	// lock locals
	// double-check whether remote locks have been requested
	// if not, request them
	numLocksNotAcquired += lm.innerLock(txn, read, txn.ReadSet)
	return numLocksNotAcquired
}

func (lm *lockManager) innerLock(txn *pb.Transaction, mode lockMode, set [][]byte) int {
	numLocksNotAcquired := 0
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
					break
				}

				// at this point I know there are a bunch of other requests sitting
				// I'm a write so I will certainly not get the lock and have to wait
				if mode == write {
					numLocksNotAcquired++
				}

				txnID, _ := ulid.ParseIdFromProto(txn.Id)
				for ; j < len(lockRequests); j++ {

					// if I'm a read and I find a write ahead of me,
					// I know I won't be getting the lock
					if mode == read && lockRequests[j].mode == write {
						numLocksNotAcquired++
					}

					id, _ := ulid.ParseIdFromProto(lockRequests[j].txn.Id)
					if txnID.CompareTo(id) == 0 {
						// it seems I requested the lock already
						break
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

	return numLocksNotAcquired
}

func (lm *lockManager) release(txn *pb.Transaction) []lockRequest {
	grantedRequests := make([]lockRequest, 0)
	// find lock that was held and release it
	grantedRequests = append(grantedRequests, lm.innerRelease(txn.Id, txn.ReadWriteSet)...)
	grantedRequests = append(grantedRequests, lm.innerRelease(txn.Id, txn.ReadSet)...)
	return grantedRequests
}

func (lm *lockManager) innerRelease(txnIDProto *pb.Id128, set [][]byte) []lockRequest {
	precededByWrite := false
	var deletedLockRequest *lockRequest
	j := 0

	for i := 0; i < len(set); i++ {
		key := set[i]
		keyHash := lm.hash(key)
		lockRequests, ok := lm.lockMap[keyHash]

		if ok {
			txnID, _ := ulid.ParseIdFromProto(txnIDProto)
			for ; j < len(lockRequests); j++ {
				id, _ := ulid.ParseIdFromProto(lockRequests[j].txn.Id)
				if lockRequests[j].mode == write {
					precededByWrite = true
				}
				if txnID.CompareTo(id) == 0 {
					break
				}
			}

			if j < len(lockRequests) {
				// remove i
				deletedLockRequest = &lockRequests[j]
				lockRequests = lm.removeIdx(lockRequests, j)
				if len(lockRequests) > 0 {
					lm.lockMap[keyHash] = lockRequests
				} else {
					delete(lm.lockMap, keyHash)
				}
			}
		}

		// subsequent transactions might be able to run now...check that as well
		// Grant subsequent request(s) if:
		//  (a) The canceled request held a write lock.
		//  (b) The canceled request held a read lock ALONE.
		//  (c) The canceled request held a write lock preceded only by read
		//      requests and followed by one or more read requests.
		if deletedLockRequest != nil && j < len(lockRequests) {
			newOwner := make([]lockRequest, 0)

			if deletedLockRequest.mode == write || (deletedLockRequest.mode == write && lockRequests[j].mode == write) {
				if lockRequests[j].mode == write {
					newOwner = append(newOwner, lockRequests[j])
				}

				for ; j < len(lockRequests) && lockRequests[j].mode == read; j++ {
					newOwner = append(newOwner, lockRequests[j])
				}
			} else if !precededByWrite && deletedLockRequest.mode == write && lockRequests[j].mode == read {
				for ; j < len(lockRequests) && lockRequests[j].mode == read; j++ {
					newOwner = append(newOwner, lockRequests[j])
				}
			}

			return newOwner
		}
	}

	return nil
}

func (lm *lockManager) removeIdx(lockRequests []lockRequest, idx int) []lockRequest {
	if idx == 0 {
		return lockRequests[1:]
	} else if idx == len(lockRequests)-1 {
		return lockRequests[:len(lockRequests)-1]
	}

	return append(lockRequests[:idx], lockRequests[idx+1:]...)
}
