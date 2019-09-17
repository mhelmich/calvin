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

package calvin

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/mhelmich/calvin/execution"
	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/scheduler"
	"github.com/mhelmich/calvin/sequencer"
	"github.com/mhelmich/calvin/util"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
)

const (
	numWorkerThreads = 2
	goodChannelSize  = numWorkerThreads*2 + 1
)

func NewCalvin(opts Options) *Calvin {
	// cip := util.NewClusterInfoProvider(opts.raftID, opts.clusterInfoPath)
	cc := util.NewConnectionCache(opts.clusterInfoProvider)

	logger := log.WithFields(log.Fields{
		"raftIdHex": hex.EncodeToString(util.Uint64ToBytes(opts.raftID)),
		"raftId":    util.Uint64ToString(opts.raftID),
	})

	srvr := grpc.NewServer()
	// myAddress := fmt.Sprintf("%s:%d", util.OutboundIP().To4().String(), cfg.Port)
	myAddress := fmt.Sprintf("%s:%d", opts.hostname, opts.port)
	logger.Infof("My address: [%s]", myAddress)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	txnBatchChan := make(chan *pb.TransactionBatch, goodChannelSize)
	peers := []raft.Peer{raft.Peer{
		ID:      opts.raftID,
		Context: []byte(myAddress),
	}}

	for idx := range opts.peers {
		addr := opts.clusterInfoProvider.GetAddressFor(opts.peers[idx])
		peers = append(peers, raft.Peer{
			ID:      opts.peers[idx],
			Context: []byte(addr),
		})
	}

	storeDir := fmt.Sprintf("%s%d", opts.storePath, opts.raftID)
	if !strings.HasSuffix(storeDir, "/") {
		storeDir = storeDir + "/"
	}
	seq := sequencer.NewSequencer(opts.raftID, txnBatchChan, peers, storeDir, cc, opts.clusterInfoProvider, srvr, opts.snapshotHandler, logger)

	// releaser might be waiting to send on ready channel
	readyTxnChan := make(chan *pb.Transaction, goodChannelSize)
	// workers might be waiting to send on done channel
	doneTxnChan := make(chan *pb.Transaction, goodChannelSize)
	sched := scheduler.NewScheduler(txnBatchChan, readyTxnChan, doneTxnChan, srvr, logger)

	engineOpts := execution.EngineOpts{
		ScheduledTxnChan: readyTxnChan,
		DoneTxnChan:      doneTxnChan,
		Stp:              opts.dataStore,
		Srvr:             srvr,
		ConnCache:        cc,
		Cip:              opts.clusterInfoProvider,
		NumWorkers:       numWorkerThreads,
		Logger:           logger,
	}
	engine := execution.NewEngine(engineOpts)

	go srvr.Serve(lis)

	return &Calvin{
		cc:           cc,
		cip:          opts.clusterInfoProvider,
		seq:          seq,
		sched:        sched,
		engine:       engine,
		grpcSrvr:     srvr,
		dataStore:    opts.dataStore,
		txnBatchChan: txnBatchChan,
		readyTxnChan: readyTxnChan,
		doneTxnChan:  doneTxnChan,
		logger:       logger,
		myRaftID:     opts.raftID,
	}
}

type Calvin struct {
	cip          util.ClusterInfoProvider
	cc           util.ConnectionCache
	seq          *sequencer.Sequencer
	sched        *scheduler.Scheduler
	engine       *execution.Engine
	grpcSrvr     *grpc.Server
	dataStore    util.DataStoreTxnProvider
	txnBatchChan chan *pb.TransactionBatch
	readyTxnChan chan *pb.Transaction
	doneTxnChan  chan *pb.Transaction
	logger       *log.Entry
	myRaftID     uint64
}

func (c *Calvin) Stop() {
	c.logger.Warningf("Shutting down calvin [%d]", c.myRaftID)
	c.grpcSrvr.Stop()
	c.seq.Stop()
	time.Sleep(time.Second)
	c.dataStore.Close()
}

func (c *Calvin) SubmitTransaction(txn *pb.Transaction) {
	c.seq.SubmitTransaction(txn)
}

func (c *Calvin) LowIsolationRead(key []byte) ([]byte, error) {
	ownerID := c.cip.FindOwnerForKey(key)
	client, err := c.cc.GetLowIsolationReadClient(ownerID)
	if err != nil {
		c.logger.Errorf("%s", err.Error())
	}

	ctx := context.Background()
	req := &pb.LowIsolationReadRequest{
		Keys: [][]byte{key},
	}
	resp, err := client.LowIsolationRead(ctx, req)
	return resp.Values[0], err
}

func (c *Calvin) LogToJSON(out io.Writer) error {
	return c.seq.LogToJSON(out, 10)
}

func (c *Calvin) LockChainToASCII(out io.Writer) {
	c.sched.LockChainToASCII(out)
}

func (c *Calvin) ChannelsToASCII(out io.Writer) {
	out.Write([]byte(fmt.Sprintf("txnBatchChan %d/%d\n", len(c.txnBatchChan), cap(c.txnBatchChan))))
	out.Write([]byte(fmt.Sprintf("readyTxnChan %d/%d\n", len(c.readyTxnChan), cap(c.readyTxnChan))))
	out.Write([]byte(fmt.Sprintf("doneTxnChan %d/%d\n", len(c.doneTxnChan), cap(c.doneTxnChan))))
}
