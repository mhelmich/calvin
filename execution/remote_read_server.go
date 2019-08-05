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
	"fmt"
	"sync"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	log "github.com/sirupsen/logrus"
)

func newRemoteReadServer(readyToExecChan chan<- *txnExecEnvironment, logger *log.Entry) *remoteReadServer {
	return &remoteReadServer{
		txnIdToTxnExecEnv: &sync.Map{},
		readyToExecChan:   readyToExecChan,
		logger:            logger,
	}
}

type txnExecEnvironment struct {
	txnId  *ulid.ID
	keys   [][]byte
	values [][]byte
	mutex  *sync.Mutex
}

func (e *txnExecEnvironment) String() string {
	return fmt.Sprintf("execEnv: %s %d %d", e.txnId.String(), len(e.keys), len(e.values))
}

type remoteReadServer struct {
	txnIdToTxnExecEnv *sync.Map // looks like map[string]*txnExecEnvironment
	readyToExecChan   chan<- *txnExecEnvironment
	logger            *log.Entry
}

func (rrs *remoteReadServer) RemoteRead(ctx context.Context, req *pb.RemoteReadRequest) (*pb.RemoteReadResponse, error) {
	id, err := ulid.ParseIdFromProto(req.TxnId)
	if err != nil {
		return nil, err
	}

	v, _ := rrs.txnIdToTxnExecEnv.LoadOrStore(id.String(), &txnExecEnvironment{
		mutex: &sync.Mutex{},
		txnId: id,
	})
	execEnv := v.(*txnExecEnvironment)

	execEnv.mutex.Lock()
	defer execEnv.mutex.Unlock()
	execEnv.keys = append(execEnv.keys, req.Keys...)
	execEnv.values = append(execEnv.values, req.Values...)

	if int(req.TotalNumLocks) == len(execEnv.keys) {
		// this txn can run
		rrs.readyToExecChan <- execEnv
		rrs.txnIdToTxnExecEnv.Delete(id.String())
	} else {
		rrs.txnIdToTxnExecEnv.Store(id.String(), execEnv)
	}

	return &pb.RemoteReadResponse{}, nil
}
