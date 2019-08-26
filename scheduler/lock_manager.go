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
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"

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
		lockMap:         make(map[uint64][]lockRequest),
		txnsToNumWaiter: make(map[*pb.Transaction]int),
		txnsToWaiters:   make(map[*pb.Transaction][]lockRequest),
		lockMgrMutex:    &sync.Mutex{},
	}
}

type lockRequest struct {
	txn  *pb.Transaction
	mode lockMode
	key  []byte
}

func (lr lockRequest) String() string {
	id, _ := ulid.ParseIdFromProto(lr.txn.Id)
	var lm string
	if lr.mode == write {
		lm = "write"
	} else {
		lm = "read"
	}
	return "[" + string(lr.key) + " - " + id.String() + " - " + lm + "]"
}

// The lockManager's lock map tracks all lock requests. For a
// given key, if 'lockMap' contains a nonempty array, then the item with
// that key is locked and either:
//  (a) first element in the array specifies the owner if that item is a
//      request for a write lock, or
//  (b) a read lock is held by all elements of the longest prefix of the array
//      containing only read lock requests.
type lockManager struct {
	lockMap         map[uint64][]lockRequest
	txnsToNumWaiter map[*pb.Transaction]int
	txnsToWaiters   map[*pb.Transaction][]lockRequest
	lockMgrMutex    *sync.Mutex
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
	lm.lockMgrMutex.Lock()
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

	if numLocksNotAcquired > 0 {
		lm.txnsToNumWaiter[txn] = numLocksNotAcquired
	}

	lm.lockMgrMutex.Unlock()
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

				// at this point I know there are a bunch of other requests waiting in line
				// I'm a write so I will certainly not get the lock and have to wait too
				if mode == write {
					numLocksNotAcquired++
				}

				for ; j < len(lockRequests); j++ {

					// if I'm a read and I find a write ahead of me,
					// I know I won't be getting the lock
					if mode == read && lockRequests[j].mode == write {
						numLocksNotAcquired++
					}

					if txn.Id.Equal(lockRequests[j].txn.Id) {
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
					lm.addWaiter(txn, req)
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
				lm.addWaiter(txn, req)
			}
		}
	}

	return numLocksNotAcquired
}

func (lm *lockManager) addWaiter(txn *pb.Transaction, req lockRequest) {
	lrs, ok := lm.txnsToWaiters[txn]
	if ok {
		lrs = append(lrs, req)
	} else {
		lrs = []lockRequest{req}
	}
	lm.txnsToWaiters[txn] = lrs
}

func (lm *lockManager) release(txn *pb.Transaction) []*pb.Transaction {
	grantedRequests := make([]lockRequest, 0)
	lm.lockMgrMutex.Lock()
	delete(lm.txnsToWaiters, txn)
	// find lock that was held and release it
	grantedRequests = append(grantedRequests, lm.innerRelease(txn.Id, txn.ReadWriteSet)...)
	grantedRequests = append(grantedRequests, lm.innerRelease(txn.Id, txn.ReadSet)...)

	newOwners := make([]*pb.Transaction, 0)
	for idx := range grantedRequests {
		numLocksNotAcquired := lm.txnsToNumWaiter[grantedRequests[idx].txn]
		if numLocksNotAcquired == 1 {
			delete(lm.txnsToNumWaiter, grantedRequests[idx].txn)
			newOwners = append(newOwners, grantedRequests[idx].txn)
		} else {
			numLocksNotAcquired--
			lm.txnsToNumWaiter[grantedRequests[idx].txn] = numLocksNotAcquired
		}
	}

	lm.lockMgrMutex.Unlock()
	return newOwners
}

func (lm *lockManager) innerRelease(txnIDProto *pb.Id128, set [][]byte) []lockRequest {
	newOwner := make([]lockRequest, 0)
	for i := 0; i < len(set); i++ {
		key := set[i]
		keyHash := lm.hash(key)
		lockRequests, ok := lm.lockMap[keyHash]
		j := 0
		precededByWrite := false
		var deletedLockRequest *lockRequest

		if ok {
			for ; j < len(lockRequests); j++ {
				if lockRequests[j].mode == write {
					precededByWrite = true
				}
				if txnIDProto.Equal(lockRequests[j].txn.Id) {
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
			if deletedLockRequest.mode == write || (deletedLockRequest.mode == write && lockRequests[j].mode == write) {
				if lockRequests[j].mode == write { // (a)
					newOwner = append(newOwner, lockRequests[j])
				}

				for ; j < len(lockRequests) && lockRequests[j].mode == read; j++ { // (b)
					newOwner = append(newOwner, lockRequests[j])
				}
			} else if !precededByWrite && deletedLockRequest.mode == write && lockRequests[j].mode == read { // (c)
				for ; j < len(lockRequests) && lockRequests[j].mode == read; j++ {
					newOwner = append(newOwner, lockRequests[j])
				}
			}
		}
	}

	return newOwner
}

// order preserving remove idx ... is probably inefficient though :/
func (lm *lockManager) removeIdx(lockRequests []lockRequest, idx int) []lockRequest {
	if idx == 0 {
		return lockRequests[1:]
	} else if idx == len(lockRequests)-1 {
		return lockRequests[:len(lockRequests)-1]
	}

	return append(lockRequests[:idx], lockRequests[idx+1:]...)
}

func (lm *lockManager) lockChainToAscii(out io.Writer) {
	lm.lockMgrMutex.Lock()

	out.Write([]byte("LOCK CHAIN:\n"))
	for _, lockRequests := range lm.lockMap {
		for i := 0; i < len(lockRequests); i++ {
			out.Write([]byte(lockRequests[i].String()))
			out.Write([]byte(" -> "))
		}
		out.Write([]byte("\n"))
	}

	out.Write([]byte("TXN WAITS:\n"))
	for k, v := range lm.txnsToNumWaiter {
		id, _ := ulid.ParseIdFromProto(k.Id)
		out.Write([]byte(fmt.Sprintf("[%s] -> [%d]\n", id, v)))
	}

	out.Write([]byte("TXN WAITERS:\n"))
	for k, lrs := range lm.txnsToWaiters {
		id, _ := ulid.ParseIdFromProto(k.Id)
		var sb strings.Builder
		for idx := range lrs {
			sb.WriteString(lrs[idx].String())
			sb.WriteString(" * ")
		}
		out.Write([]byte(fmt.Sprintf("[%s] -> [%s]\n", id, sb.String())))
	}

	lm.lockMgrMutex.Unlock()
}
