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
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
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
	txnIDStr := id.String()

	if len(req.Keys) <= 0 {
		rrs.logger.Warningf("received empty remote read for [%s]", txnIDStr)
		return &pb.RemoteReadResponse{}, nil
	}

	defer util.TrackTime(rrs.logger, fmt.Sprintf("RemoteRead [%s]", txnIDStr), time.Now())
	v, _ := rrs.txnIdToTxnExecEnv.LoadOrStore(txnIDStr, &txnExecEnvironment{
		mutex: &sync.Mutex{},
		txnId: id,
	})
	execEnv := v.(*txnExecEnvironment)

	execEnv.mutex.Lock()
	execEnv.keys = append(execEnv.keys, req.Keys...)
	execEnv.values = append(execEnv.values, req.Values...)

	if int(req.TotalNumLocks) == len(execEnv.keys) {
		// this txn can run
		rrs.logger.Debugf("txn [%s] can run [%d] [%d] [%d]", txnIDStr, int(req.TotalNumLocks), len(execEnv.keys), len(req.Keys))
		rrs.txnIdToTxnExecEnv.Delete(txnIDStr)
		rrs.readyToExecChan <- execEnv
	} else {
		// stashing remote reads for a later delivery of remote reads
		rrs.logger.Debugf("stashing remote reads for txn [%s]", txnIDStr)
		rrs.txnIdToTxnExecEnv.Store(txnIDStr, execEnv)
	}

	execEnv.mutex.Unlock()
	return &pb.RemoteReadResponse{}, nil
}
