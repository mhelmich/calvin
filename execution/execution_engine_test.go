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
	"testing"
	"time"

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

func TestEngineBasic(t *testing.T) {
	scheduledTxnChan := make(chan *pb.Transaction)
	mockDS := new(mocks.Storage)
	mockDS.On("Get", mock.AnythingOfType("[]uint8")).Return(
		func(b []byte) []byte { return []byte(string(b) + "_value") },
	)
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

	e := NewEngine(scheduledTxnChan, mockDS, srvr, mockCC, mockCIP, log.WithFields(log.Fields{}))

	scheduledTxnChan <- &pb.Transaction{
		ReadSet:      [][]byte{[]byte("moep")},
		ReadWriteSet: [][]byte{[]byte("narf")},
		WriterNodes:  []uint64{99},
	}

	time.Sleep(10 * time.Millisecond)
	e.Stop()
}
