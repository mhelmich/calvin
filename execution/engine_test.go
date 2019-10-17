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

import (
	"context"
	"sync"
	"testing"

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	glua "github.com/yuin/gopher-lua"
	"google.golang.org/grpc"
)

func TestEngineBasic(t *testing.T) {
	scheduledTxnChan := make(chan *pb.Transaction)
	doneTxnChan := make(chan *pb.Transaction)
	srvr := grpc.NewServer()

	mockRRC := new(mocks.RemoteReadClient)
	mockRRC.On("RemoteRead", mock.Anything, mock.AnythingOfType("*pb.RemoteReadRequest")).Run(
		func(args mock.Arguments) {
			req := args[1].(*pb.RemoteReadRequest)
			assert.Equal(t, 2, len(req.Keys))
			assert.Equal(t, 2, len(req.Values))
			assert.Equal(t, []byte("moep"), req.Keys[0])
			assert.Equal(t, []byte("moep_value"), req.Values[0])
			assert.Equal(t, []byte("narf"), req.Keys[1])
			assert.Equal(t, []byte("narf_value"), req.Values[1])
			assert.Equal(t, uint32(2), req.TotalNumLocks)
		},
	).Return(
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) *pb.RemoteReadResponse {
			return &pb.RemoteReadResponse{}
		},
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) error { return nil },
	)

	mockCC := new(mocks.ConnectionCache)
	mockCC.On("GetRemoteReadClient", mock.AnythingOfType("uint64")).Return(mockRRC, nil)

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) bool { return "narf" == string(b) || "moep" == string(b) },
	)
	mockCIP.On("AmIWriter", mock.AnythingOfType("[]uint64")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Get", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) []byte { return []byte(string(b) + "_value") },
	)
	mockTxn.On("Rollback").Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", mock.AnythingOfType("bool")).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)

	opts := EngineOpts{
		ScheduledTxnChan: scheduledTxnChan,
		DoneTxnChan:      doneTxnChan,
		PartitionedStore: mockStore,
		Srvr:             srvr,
		ConnCache:        mockCC,
		Cip:              mockCIP,
		NumWorkers:       2,
		Logger:           log.WithFields(log.Fields{}),
	}
	NewEngine(opts)

	txnID, err := ulid.NewId()
	assert.Nil(t, err)
	scheduledTxnChan <- &pb.Transaction{
		Id:           txnID.ToProto(),
		ReadSet:      [][]byte{[]byte("moep")},
		ReadWriteSet: [][]byte{[]byte("narf")},
		WriterNodes:  []uint64{99},
	}

	close(scheduledTxnChan)
}

func TestWorkerBasic(t *testing.T) {
	scheduledTxnChan := make(chan *pb.Transaction)
	readyToExecChan := make(chan *txnExecEnvironment, 1)
	doneTxnChan := make(chan *pb.Transaction)

	mockRRC := new(mocks.RemoteReadClient)
	mockRRC.On("RemoteRead", mock.Anything, mock.AnythingOfType("*pb.RemoteReadRequest")).Run(
		func(args mock.Arguments) {
			req := args[1].(*pb.RemoteReadRequest)
			assert.Equal(t, 2, len(req.Keys))
			assert.Equal(t, 2, len(req.Values))
			assert.Equal(t, []byte("moep"), req.Keys[0])
			assert.Equal(t, []byte("moep_value"), req.Values[0])
			assert.Equal(t, []byte("narf"), req.Keys[1])
			assert.Equal(t, []byte("narf_value"), req.Values[1])
			assert.Equal(t, uint32(2), req.TotalNumLocks)
		},
	).Return(
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) *pb.RemoteReadResponse {
			return &pb.RemoteReadResponse{}
		},
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) error { return nil },
	)

	mockCC := new(mocks.ConnectionCache)
	mockCC.On("GetRemoteReadClient", mock.AnythingOfType("uint64")).Return(mockRRC, nil)

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) bool { return "narf" == string(b) || "moep" == string(b) },
	)
	mockCIP.On("AmIWriter", mock.AnythingOfType("[]uint64")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Get", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) []byte { return []byte(string(b) + "_value") },
	)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", mock.AnythingOfType("bool")).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)

	txnsToExecute := &sync.Map{}
	logger := log.WithFields(log.Fields{})
	procs := &sync.Map{}
	initStoredProcedures(procs)

	counter := uint64(0)
	w := worker{
		scheduledTxnChan:    scheduledTxnChan,
		readyToExecChan:     readyToExecChan,
		doneTxnChan:         doneTxnChan,
		partitionedStore:    mockStore,
		connCache:           mockCC,
		cip:                 mockCIP,
		txnsToExecute:       txnsToExecute,
		storedProcs:         procs,
		compiledStoredProcs: make(map[string]*glua.LFunction),
		luaState:            glua.NewState(),
		counter:             &counter,
		logger:              logger,
	}
	go w.runWorker()

	id, err := ulid.NewId()
	assert.Nil(t, err)

	txn := &pb.Transaction{
		Id:              id.ToProto(),
		StoredProcedure: simpleSetterProcName,
	}
	txnsToExecute.Store(id.String(), txn)

	readyToExecChan <- &txnExecEnvironment{
		txnId: id,
	}

	doneTxn := <-doneTxnChan
	doneID, err := ulid.ParseIdFromProto(doneTxn.Id)
	assert.Nil(t, err)
	assert.Equal(t, id.String(), doneID.String())
	close(scheduledTxnChan)
}

func TestWorkerSimpleSetter(t *testing.T) {
	scheduledTxnChan := make(chan *pb.Transaction)
	readyToExecChan := make(chan *txnExecEnvironment, 1)
	doneTxnChan := make(chan *pb.Transaction)

	mockRRC := new(mocks.RemoteReadClient)
	mockRRC.On("RemoteRead", mock.Anything, mock.AnythingOfType("*pb.RemoteReadRequest")).Run(
		func(args mock.Arguments) {
			req := args[1].(*pb.RemoteReadRequest)
			assert.Equal(t, 2, len(req.Keys))
			assert.Equal(t, 2, len(req.Values))
			assert.Equal(t, []byte("moep"), req.Keys[0])
			assert.Equal(t, []byte("moep_value"), req.Values[0])
			assert.Equal(t, []byte("narf"), req.Keys[1])
			assert.Equal(t, []byte("narf_value"), req.Values[1])
			assert.Equal(t, uint32(2), req.TotalNumLocks)
		},
	).Return(
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) *pb.RemoteReadResponse {
			return &pb.RemoteReadResponse{}
		},
		func(arg1 context.Context, arg2 *pb.RemoteReadRequest, arg3 ...grpc.CallOption) error { return nil },
	)

	mockCC := new(mocks.ConnectionCache)
	mockCC.On("GetRemoteReadClient", mock.AnythingOfType("uint64")).Return(mockRRC, nil)

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) bool { return "narf" == string(b) || "moep" == string(b) },
	)
	mockCIP.On("AmIWriter", mock.AnythingOfType("[]uint64")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxn.On("Commit").Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)

	txnsToExecute := &sync.Map{}
	logger := log.WithFields(log.Fields{})

	procs := &sync.Map{}
	initStoredProcedures(procs)

	counter := uint64(0)
	w := worker{
		scheduledTxnChan:    scheduledTxnChan,
		readyToExecChan:     readyToExecChan,
		doneTxnChan:         doneTxnChan,
		connCache:           mockCC,
		cip:                 mockCIP,
		partitionedStore:    mockStore,
		txnsToExecute:       txnsToExecute,
		storedProcs:         procs,
		compiledStoredProcs: make(map[string]*glua.LFunction),
		luaState:            glua.NewState(),
		counter:             &counter,
		logger:              logger,
	}
	go w.runWorker()

	id, err := ulid.NewId()
	assert.Nil(t, err)

	arg := &pb.SimpleSetterArg{
		Key:   []byte("narf"),
		Value: []byte("narf_value"),
	}
	argBites, err := arg.Marshal()
	assert.Nil(t, err)

	txn := &pb.Transaction{
		Id:                  id.ToProto(),
		StoredProcedure:     simpleSetterProcName,
		StoredProcedureArgs: [][]byte{argBites},
	}
	txnsToExecute.Store(id.String(), txn)

	readyToExecChan <- &txnExecEnvironment{
		txnId:  id,
		keys:   [][]byte{[]byte("narf")},
		values: [][]byte{[]byte("narf_value")},
	}

	doneTxn := <-doneTxnChan
	doneID, err := ulid.ParseIdFromProto(doneTxn.Id)
	assert.Nil(t, err)
	assert.Equal(t, id.String(), doneID.String())
	close(scheduledTxnChan)
}
