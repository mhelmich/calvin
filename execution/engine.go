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
	"sync/atomic"
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	glua "github.com/yuin/gopher-lua"
	"google.golang.org/grpc"
	gluar "layeh.com/gopher-luar"
)

func NewEngine(scheduledTxnChan <-chan *pb.Transaction, doneTxnChan chan<- *pb.Transaction, store util.DataStore, srvr *grpc.Server, connCache util.ConnectionCache, cip util.ClusterInfoProvider, logger *log.Entry) *Engine {
	readyToExecChan := make(chan *txnExecEnvironment, 1)

	rrs := newRemoteReadServer(readyToExecChan, logger)
	pb.RegisterRemoteReadServer(srvr, rrs)
	txnsToExecute := &sync.Map{}
	storedProcs := &sync.Map{}
	initStoredProcedures(storedProcs)
	compiledStoredProcs := &sync.Map{}
	counter := uint64(0)

	for i := 0; i < 2; i++ {
		w := worker{
			scheduledTxnChan:    scheduledTxnChan,
			readyToExecChan:     readyToExecChan,
			doneTxnChan:         doneTxnChan,
			store:               store,
			connCache:           connCache,
			cip:                 cip,
			txnsToExecute:       txnsToExecute,
			storedProcs:         storedProcs,
			luaState:            glua.NewState(),
			compiledStoredProcs: compiledStoredProcs,
			logger:              logger,
			counter:             &counter,
		}
		go w.runWorker()
	}

	e := &Engine{
		storedProcs: storedProcs,
		counter:     &counter,
	}

	if log.GetLevel() == log.DebugLevel {
		go func() {
			for {
				c := atomic.LoadUint64(e.counter)
				logger.Debugf("processed %d txns", c)
				time.Sleep(time.Second)
			}
		}()
	}

	return e
}

type Engine struct {
	storedProcs *sync.Map
	counter     *uint64
}

type worker struct {
	scheduledTxnChan    <-chan *pb.Transaction
	readyToExecChan     <-chan *txnExecEnvironment
	doneTxnChan         chan<- *pb.Transaction
	store               util.DataStore
	connCache           util.ConnectionCache
	cip                 util.ClusterInfoProvider
	txnsToExecute       *sync.Map
	storedProcs         *sync.Map
	compiledStoredProcs *sync.Map
	luaState            *glua.LState
	logger              *log.Entry
	counter             *uint64
}

func (w *worker) runWorker() {
	for {
		select {
		// wait for txns to be scheduled
		case txn := <-w.scheduledTxnChan:
			if txn == nil {
				w.logger.Warningf("Execution worker shutting down")
				w.luaState.Close()
				return
			}
			w.processScheduledTxn(txn, w.store)

		// wait for remote reads to be collected
		case execEnv := <-w.readyToExecChan:
			w.runReadyTxn(execEnv)
			atomic.AddUint64(w.counter, uint64(1))

		}
	}
}

func (w *worker) processScheduledTxn(txn *pb.Transaction, store util.DataStore) {
	localKeys := make([][]byte, 0)
	localValues := make([][]byte, 0)
	// do local reads
	dsTxn, err := store.StartTxn(false)
	if err != nil {
		w.logger.Panicf("Can't start txn: %s", err.Error())
	}
	for idx := range txn.ReadSet {
		key := txn.ReadSet[idx]
		if w.cip.IsLocal(key) {
			value := store.Get(key)
			localKeys = append(localKeys, key)
			localValues = append(localValues, value)
		}
	}
	for idx := range txn.ReadWriteSet {
		key := txn.ReadWriteSet[idx]
		if w.cip.IsLocal(key) {
			value := store.Get(key)
			localKeys = append(localKeys, key)
			localValues = append(localValues, value)
		}
	}
	err = dsTxn.Rollback()
	if err != nil {
		w.logger.Panicf("Can't roll back txn: %s", err.Error())
	}

	if w.cip.AmIWriter(txn.WriterNodes) {
		id, err := ulid.ParseIdFromProto(txn.Id)
		if err != nil {
			w.logger.Panicf("%s\n", err.Error())
		}

		w.txnsToExecute.Store(id.String(), txn)
	}

	// broadcast remote reads to all write peers
	w.broadcastLocalReadsToWriterNodes(txn, localKeys, localValues)
}

func (w *worker) broadcastLocalReadsToWriterNodes(txn *pb.Transaction, keys [][]byte, values [][]byte) {
	defer util.TrackTime(w.logger, "broadcastLocalReadsToWriterNodes", time.Now())
	for idx := range txn.WriterNodes {
		client, err := w.connCache.GetRemoteReadClient(txn.WriterNodes[idx])
		if err != nil {
			w.logger.Panicf("%s\n", err.Error())
		}

		if log.GetLevel() == log.DebugLevel {
			id, _ := ulid.ParseIdFromProto(txn.Id)
			w.logger.Debugf("broadcasting remote reads for [%s] to %d", id.String(), txn.WriterNodes[idx])
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.RemoteRead(ctx, &pb.RemoteReadRequest{
			TxnId:         txn.Id,
			TotalNumLocks: uint32(len(txn.ReadWriteSet) + len(txn.ReadSet)),
			Keys:          keys,
			Values:        values,
		})
		if err != nil || resp.Error != "" {
			w.logger.Errorf("Node [%d] wasn't reachable: %s\n", txn.WriterNodes[idx], err.Error())
		}
	}
}

func (w *worker) runReadyTxn(execEnv *txnExecEnvironment) {
	defer util.TrackTime(w.logger, "runReadyTxn", time.Now())
	t, ok := w.txnsToExecute.Load(execEnv.txnId.String())
	if !ok {
		w.logger.Panicf("Can't find txn [%s]\n", execEnv.txnId.String())
	}
	w.txnsToExecute.Delete(execEnv.txnId.String())
	txn := t.(*pb.Transaction)

	w.runTxn(txn, execEnv)
	w.doneTxnChan <- txn
}

func (w *worker) runTxn(txn *pb.Transaction, execEnv *txnExecEnvironment) error {
	defer util.TrackTime(w.logger, "runTxn", time.Now())
	lds := &luaDataStore{
		ds:     w.store,
		keys:   execEnv.keys,
		values: execEnv.values,
		cip:    w.cip,
	}

	dsTxn, err := lds.StartTxn(true)
	if err != nil {
		return err
	}
	err = w.runLua(txn, execEnv, lds)
	if err != nil {
		err2 := dsTxn.Rollback()
		if err2 != nil {
			return fmt.Errorf("%s %s", err.Error(), err2.Error())
		}
		return err
	}

	return dsTxn.Commit()
}

func (w *worker) runLua(txn *pb.Transaction, execEnv *txnExecEnvironment, lds *luaDataStore) error {
	fction, ok := w.compiledStoredProcs.Load(txn.StoredProcedure)
	if !ok {
		v, ok := w.storedProcs.Load(txn.StoredProcedure)
		if !ok {
			return fmt.Errorf("Can't find proc [%s]", txn.StoredProcedure)
		}
		script := v.(string)

		fn, err := w.luaState.LoadString(script)
		if err != nil {
			w.logger.Panicf("%s\n", err.Error())
		}

		fction, _ = w.compiledStoredProcs.LoadOrStore(txn.StoredProcedure, fn)
	}

	args := w.convertByteArrayToStringArray(txn.StoredProcedureArgs)
	keys := w.convertByteArrayToStringArray(execEnv.keys)

	w.luaState.SetGlobal("store", gluar.New(w.luaState, lds))
	w.luaState.SetGlobal("KEYC", gluar.New(w.luaState, len(keys)))
	w.luaState.SetGlobal("KEYV", gluar.New(w.luaState, keys))
	w.luaState.SetGlobal("ARGC", gluar.New(w.luaState, len(args)))
	w.luaState.SetGlobal("ARGV", gluar.New(w.luaState, args))

	fn := fction.(*glua.LFunction)
	w.luaState.Push(fn)
	return w.luaState.PCall(0, glua.MultRet, nil)
}

func (w *worker) convertByteArrayToStringArray(args [][]byte) []string {
	strs := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		strs[i] = string(args[i])
	}
	return strs
}
