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

func newLuaState() *glua.LState {
	opts := glua.Options{
		SkipOpenLibs: true,
	}
	state := glua.NewState(opts)
	glua.OpenPackage(state)
	glua.OpenBase(state)
	glua.OpenTable(state)
	glua.OpenString(state)
	glua.OpenMath(state)
	return state
}

type EngineOpts struct {
	ScheduledTxnChan <-chan *pb.Transaction
	DoneTxnChan      chan<- *pb.Transaction
	PartitionedStore util.PartitionedDataStore
	Srvr             *grpc.Server
	ConnCache        util.ConnectionCache
	Cip              util.ClusterInfoProvider
	NumWorkers       int
	Logger           *log.Entry
}

func NewEngine(opts EngineOpts) *Engine {
	readyToExecChan := make(chan *txnExecEnvironment, opts.NumWorkers*2+1)

	rrs := newRemoteReadServer(readyToExecChan, opts.Logger)
	pb.RegisterRemoteReadServer(opts.Srvr, rrs)
	txnsToExecute := &sync.Map{}
	storedProcs := &sync.Map{}
	initStoredProcedures(storedProcs)
	counter := uint64(0)

	for i := 0; i < opts.NumWorkers; i++ {
		w := worker{
			scheduledTxnChan: opts.ScheduledTxnChan,
			readyToExecChan:  readyToExecChan,
			doneTxnChan:      opts.DoneTxnChan,
			connCache:        opts.ConnCache,
			cip:              opts.Cip,
			txnsToExecute:    txnsToExecute,
			storedProcs:      storedProcs,
			partitionedStore: opts.PartitionedStore,
			luaState:         newLuaState(),
			// premature optimization came back to haunt me
			// each worker needs its own compiledStoredProc map
			// because compilation happens in relationship to a
			// lua state
			// and because all workers have their own lua state,
			// all workers need their own procedure cache
			compiledStoredProcs: make(map[string]*glua.LFunction),
			logger:              opts.Logger,
			counter:             &counter,
		}
		go w.runWorker()
	}

	e := &Engine{
		storedProcs: storedProcs,
		counter:     &counter,
	}

	// DEBUG
	if log.GetLevel() == log.DebugLevel {
		go func() {
			for {
				c := atomic.LoadUint64(e.counter)
				opts.Logger.Debugf("processed %d txns", c)
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
	connCache           util.ConnectionCache
	cip                 util.ClusterInfoProvider
	txnsToExecute       *sync.Map
	storedProcs         *sync.Map
	compiledStoredProcs map[string]*glua.LFunction
	luaState            *glua.LState
	partitionIDToTxn    map[int]util.DataStoreTxn
	partitionedStore    util.PartitionedDataStore
	logger              *log.Entry
	counter             *uint64
}

func (w *worker) runWorker() {
	luaStateRenewTicker := time.NewTicker(time.Minute)
	defer luaStateRenewTicker.Stop()

	for {
		select {
		// wait for txns to be scheduled
		case txn := <-w.scheduledTxnChan:
			if txn == nil {
				w.logger.Warningf("Execution worker shutting down")
				w.luaState.Close()
				return
			}

			if txn.IsLowIsolationRead {
				// id, _ := ulid.ParseIdFromProto(txn.Id)
				// w.logger.Debugf("txn [%s] is low iso read?!?!?", id.String())
				w.processLowIsolationRead(txn)
			} else {
				w.processScheduledTxn(txn)
			}

		// wait for remote reads to be collected
		case execEnv := <-w.readyToExecChan:
			w.runReadyTxn(execEnv)
			atomic.AddUint64(w.counter, uint64(1))

		case <-luaStateRenewTicker.C:
			w.renewLuaState()

		}
	}
}

func (w *worker) renewLuaState() {
	defer util.TrackTime(w.logger, "renewLuaState", time.Now())
	w.luaState.Close()
	w.luaState = newLuaState()
	w.compiledStoredProcs = make(map[string]*glua.LFunction)
}

// low iso reads only need to do local reads as it is assumed this node owns the key
func (w *worker) processLowIsolationRead(txn *pb.Transaction) {
	localKeys, localValues := w.doLocalReads(txn)
	txn.LowIsolationReadResponse = &pb.LowIsolationReadResponse{
		Keys:   localKeys,
		Values: localValues,
	}
	w.doneTxnChan <- txn
}

func (w *worker) processScheduledTxn(txn *pb.Transaction) {
	localKeys, localValues := w.doLocalReads(txn)

	txnID, err := ulid.ParseIdFromProto(txn.Id)
	if err != nil {
		w.logger.Panicf("%s\n", err.Error())
	}
	txnIDStr := txnID.String()

	if w.cip.AmIWriter(txn.WriterNodes) {
		// stash this anyways to dedup on receiving a ready txn
		w.logger.Debugf("setting [%s] into txnsToExecute", txnIDStr)
		w.txnsToExecute.Store(txnIDStr, txn)
	} else {
		// if I'm not a writer, I'm done now
		// and can tell the lock manager to release the locks
		w.logger.Debugf("releasing locks for RO txn [%s]", txnIDStr)
		w.doneTxnChan <- txn
	}

	// broadcast remote reads to all write peers
	if len(localKeys) > 0 {
		w.broadcastLocalReadsToWriterNodes(txn, localKeys, localValues, txnIDStr)
	}
}

func (w *worker) doLocalReads(txn *pb.Transaction) ([][]byte, [][]byte) {
	localKeys := make([][]byte, 0)
	localValues := make([][]byte, 0)
	w.partitionIDToTxn = make(map[int]util.DataStoreTxn)

	// do local reads
	for idx := range txn.ReadSet {
		key := txn.ReadSet[idx]
		if w.cip.IsLocal(key) {
			dsTxn, err := w.getTxnForKey(key, false)
			if err != nil {
				w.logger.Panicf("can't get txn for key [%s]: %s", string(key), err.Error())
			}

			value := dsTxn.Get(key)
			localKeys = append(localKeys, key)
			localValues = append(localValues, value)
		}
	}

	for idx := range txn.ReadWriteSet {
		key := txn.ReadWriteSet[idx]
		if w.cip.IsLocal(key) {
			dsTxn, err := w.getTxnForKey(key, false)
			if err != nil {
				w.logger.Panicf("can't get txn for key [%s]: %s", string(key), err.Error())
			}

			value := dsTxn.Get(key)
			localKeys = append(localKeys, key)
			localValues = append(localValues, value)
		}
	}

	err := w.rollback()
	if err != nil {
		w.logger.Panicf("Can't roll back txn: %s", err.Error())
	}

	w.partitionIDToTxn = nil
	return localKeys, localValues
}

func (w *worker) getTxnForKey(key []byte, writable bool) (util.DataStoreTxn, error) {
	partitionID := w.cip.FindPartitionForKey(key)
	var txn util.DataStoreTxn
	var err error
	var ok bool
	var txnProvider util.DataStoreTxnProvider
	txn, ok = w.partitionIDToTxn[partitionID]
	if !ok {
		txnProvider, err = w.partitionedStore.GetPartition(partitionID)
		if err != nil {
			return nil, err
		}

		txn, err = txnProvider.StartTxn(writable)
		if err != nil {
			return nil, err
		}

		w.partitionIDToTxn[partitionID] = txn
	}

	return txn, nil
}

func (w *worker) rollback() error {
	defer func() { w.partitionIDToTxn = nil }()

	for _, txn := range w.partitionIDToTxn {
		err := txn.Rollback()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *worker) broadcastLocalReadsToWriterNodes(txn *pb.Transaction, keys [][]byte, values [][]byte, txnID string) {
	defer util.TrackTime(w.logger, fmt.Sprintf("broadcastLocalReadsToWriterNodes [%s]", txnID), time.Now())
	for idx := range txn.WriterNodes {
		client, err := w.connCache.GetRemoteReadClient(txn.WriterNodes[idx])
		if err != nil {
			w.logger.Panicf("%s\n", err.Error())
		}

		if log.GetLevel() == log.DebugLevel {
			w.logger.Debugf("broadcasting remote reads for [%s] to %d", txnID, txn.WriterNodes[idx])
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	txnID := execEnv.txnId.String()
	defer util.TrackTime(w.logger, fmt.Sprintf("runReadyTxn [%s]", txnID), time.Now())
	t, ok := w.txnsToExecute.Load(txnID)
	if !ok {
		w.logger.Panicf("Can't find txn [%s]", txnID)
	}
	w.txnsToExecute.Delete(txnID)
	txn := t.(*pb.Transaction)

	err := w.runTxn(txn, execEnv, txnID)
	if err != nil {
		w.logger.Panicf("%s", err.Error())
	}
	w.doneTxnChan <- txn
}

func (w *worker) runTxn(txn *pb.Transaction, execEnv *txnExecEnvironment, txnID string) error {
	defer util.TrackTime(w.logger, fmt.Sprintf("runTxn [%s]", txnID), time.Now())
	lds := newStoredProcDataStore(w.partitionedStore, execEnv.keys, execEnv.values, w.cip)

	err := w.runLua(txn, execEnv, lds)
	if err != nil {
		err2 := lds.rollback()
		if err2 != nil {
			return fmt.Errorf("%s %s", err.Error(), err2.Error())
		}
		return fmt.Errorf("error running lua: %s", err.Error())
	}

	return lds.commit()
}

func (w *worker) runLua(txn *pb.Transaction, execEnv *txnExecEnvironment, lds *storedProcDataStore) error {
	fction, ok := w.compiledStoredProcs[txn.StoredProcedure]
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

		w.compiledStoredProcs[txn.StoredProcedure] = fn
		fction = fn
	}

	keys := w.convertByteArrayToStringArray(execEnv.keys)
	args := w.convertBitesToArgs(txn.StoredProcedureArgs)

	// for idx := range args {
	// 	w.logger.Errorf("ARGS: %s %s", args[idx].Key, args[idx].Value)
	// }

	w.luaState.SetGlobal("store", gluar.New(w.luaState, lds))
	w.luaState.SetGlobal("KEYC", gluar.New(w.luaState, len(keys)))
	w.luaState.SetGlobal("KEYV", gluar.New(w.luaState, keys))
	w.luaState.SetGlobal("ARGC", gluar.New(w.luaState, len(args)))
	w.luaState.SetGlobal("ARGV", gluar.New(w.luaState, args))

	w.luaState.Push(fction)
	return w.luaState.PCall(0, glua.MultRet, nil)
}

func (w *worker) convertBitesToArgs(args [][]byte) []*ssa {
	ssas := make([]*ssa, len(args))
	for idx := range args {
		arg := &pb.SimpleSetterArg{}
		err := arg.Unmarshal(args[idx])
		if err != nil {
			w.logger.Panicf("%s", err.Error())
		}
		ssas[idx] = &ssa{
			Key:   string(arg.Key),
			Value: string(arg.Value),
		}
	}
	return ssas
}

type ssa struct {
	Key   string
	Value string
}

func (w *worker) convertByteArrayToStringArray(args [][]byte) []string {
	strs := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		strs[i] = string(args[i])
	}
	return strs
}
