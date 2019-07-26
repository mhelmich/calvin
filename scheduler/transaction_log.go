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
	"github.com/mhelmich/calvin/pb"
)

var transactionLog = &txnLog{
	m: make(map[uint64]*pb.TransactionBatch),
}

type txnLog struct {
	m              map[uint64]*pb.TransactionBatch
	lastCleanedIdx uint64
}

func (tl *txnLog) addBatch(batch *pb.TransactionBatch) {
	tl.m[batch.Epoch] = batch
}

func (tl *txnLog) getIndex(idx uint64) *pb.TransactionBatch {
	batch, ok := tl.m[idx]
	if !ok {
		return nil
	}

	if tl.lastCleanedIdx+10 < idx {
		go tl.cleanup(idx)
	}

	return batch
}

func (tl *txnLog) cleanup(idx uint64) {
	for i := tl.lastCleanedIdx; i < idx; i++ {
		delete(tl.m, i)
	}
	tl.lastCleanedIdx = idx
}
