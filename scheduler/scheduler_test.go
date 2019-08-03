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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestSchedulerBasic(t *testing.T) {
	sequencerChan := make(chan *pb.TransactionBatch, 1)
	readyTxns := make(chan *pb.Transaction, 1)
	doneTxnChan := make(chan *pb.Transaction, 1)
	NewScheduler(sequencerChan, readyTxns, doneTxnChan, grpc.NewServer(), log.WithFields(log.Fields{
		"component": "scheduler",
	}))
	close(sequencerChan)
}

func TestSchedulerLocking(t *testing.T) {
	sequencerChan := make(chan *pb.TransactionBatch, 3)
	readyTxns := make(chan *pb.Transaction, 3)
	doneTxnChan := make(chan *pb.Transaction, 3)
	NewScheduler(sequencerChan, readyTxns, doneTxnChan, grpc.NewServer(), log.WithFields(log.Fields{
		"component": "scheduler",
	}))

	id1, err := ulid.NewId()
	assert.Nil(t, err)
	txn1 := &pb.Transaction{
		Id:           id1.ToProto(),
		ReadWriteSet: [][]byte{[]byte("key1")},
	}

	batch1 := &pb.TransactionBatch{
		Transactions: []*pb.Transaction{txn1},
	}

	id2, err := ulid.NewId()
	assert.Nil(t, err)
	txn2 := &pb.Transaction{
		Id:           id2.ToProto(),
		ReadWriteSet: [][]byte{[]byte("key1")},
	}

	batch2 := &pb.TransactionBatch{
		Transactions: []*pb.Transaction{txn2},
	}

	fmt.Printf("put txn [%s] into sequence\n", id1.String())
	sequencerChan <- batch1
	fmt.Printf("put txn [%s] into sequence\n", id2.String())
	sequencerChan <- batch2

	readyTxn1 := <-readyTxns
	readyId1, err := ulid.ParseIdFromProto(readyTxn1.Id)
	assert.Nil(t, err)
	assert.Equal(t, id1.String(), readyId1.String())
	fmt.Printf("processed txn [%s]\n", readyId1)
	doneTxnChan <- txn1

	readyTxn2 := <-readyTxns
	readyId2, err := ulid.ParseIdFromProto(readyTxn2.Id)
	assert.Nil(t, err)
	assert.Equal(t, id2.String(), readyId2.String())
	fmt.Printf("processed txn [%s]\n", readyId2)
	doneTxnChan <- txn2

	close(sequencerChan)
	close(doneTxnChan)
}
