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
	"os"
	"strings"
	"time"

	"github.com/mhelmich/calvin/execution"
	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/scheduler"
	"github.com/mhelmich/calvin/sequencer"
	"github.com/mhelmich/calvin/util"
	"github.com/naoina/toml"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
)

const (
	smallChannelSize = 3
)

type config struct {
	RaftID    uint64
	Hostname  string
	Port      int
	StorePath string
	Peers     []uint64
}

func NewCalvin(configPath string, clusterInfoPath string) *Calvin {
	cfg := readConfig(configPath)
	cip := util.NewClusterInfoProvider(cfg.RaftID, clusterInfoPath)
	cc := util.NewConnectionCache(cip)

	logger := log.WithFields(log.Fields{
		"raftIdHex": hex.EncodeToString(util.Uint64ToBytes(cfg.RaftID)),
		"raftId":    util.Uint64ToString(cfg.RaftID),
	})

	srvr := grpc.NewServer()
	// myAddress := fmt.Sprintf("%s:%d", util.OutboundIP().To4().String(), cfg.Port)
	myAddress := fmt.Sprintf("%s:%d", cfg.Hostname, cfg.Port)
	logger.Infof("My address: [%s]", myAddress)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		logger.Panicf("%s\n", err.Error())
	}

	txnBatchChan := make(chan *pb.TransactionBatch, smallChannelSize)
	peers := []raft.Peer{raft.Peer{
		ID:      cfg.RaftID,
		Context: []byte(myAddress),
	}}

	for idx := range cfg.Peers {
		addr := cip.GetAddressFor(cfg.Peers[idx])
		peers = append(peers, raft.Peer{
			ID:      cfg.Peers[idx],
			Context: []byte(addr),
		})
	}

	storeDir := fmt.Sprintf("%s%d", cfg.StorePath, cfg.RaftID)
	if !strings.HasSuffix(storeDir, "/") {
		storeDir = storeDir + "/"
	}
	seq := sequencer.NewSequencer(cfg.RaftID, txnBatchChan, peers, storeDir, cc, cip, srvr, logger)

	readyTxnChan := make(chan *pb.Transaction, smallChannelSize)
	doneTxnChan := make(chan *pb.Transaction, smallChannelSize)
	sched := scheduler.NewScheduler(txnBatchChan, readyTxnChan, doneTxnChan, srvr, logger)

	dataStore := newBoltDataStore(storeDir, logger)
	engine := execution.NewEngine(readyTxnChan, doneTxnChan, dataStore, srvr, cc, cip, logger)

	go srvr.Serve(lis)

	return &Calvin{
		cc:           cc,
		cip:          cip,
		seq:          seq,
		sched:        sched,
		engine:       engine,
		grpcSrvr:     srvr,
		dataStore:    dataStore,
		txnBatchChan: txnBatchChan,
		readyTxnChan: readyTxnChan,
		doneTxnChan:  doneTxnChan,
		logger:       logger,
		myRaftID:     cfg.RaftID,
	}
}

type Calvin struct {
	cip          util.ClusterInfoProvider
	cc           util.ConnectionCache
	seq          *sequencer.Sequencer
	sched        *scheduler.Scheduler
	engine       *execution.Engine
	grpcSrvr     *grpc.Server
	dataStore    *boltDataStore
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
	c.dataStore.close()
}

func (c *Calvin) SubmitTransaction(txn *pb.Transaction) {
	c.seq.SubmitTransaction(txn)
}

func (c *Calvin) LowIsolationRead(key []byte) ([]byte, error) {
	ownerID := c.cip.FindOwnerFor(key)
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

func (c *Calvin) LogToJson(out io.Writer) error {
	return c.seq.LogToJson(out, 10)
}

func (c *Calvin) LockChainToAscii(out io.Writer) {
	c.sched.LockChainToAscii(out)
}

func (c *Calvin) ChannelsToAscii(out io.Writer) {
	out.Write([]byte(fmt.Sprintf("txnBatchChan %d/%d\n", len(c.txnBatchChan), cap(c.txnBatchChan))))
	out.Write([]byte(fmt.Sprintf("readyTxnChan %d/%d\n", len(c.readyTxnChan), cap(c.readyTxnChan))))
	out.Write([]byte(fmt.Sprintf("doneTxnChan %d/%d\n", len(c.doneTxnChan), cap(c.doneTxnChan))))
}

func readConfig(path string) config {
	f, err := os.Open(path)
	if err != nil {
		log.Panicf("%s\n", err.Error())
	}
	defer f.Close()

	var config config
	if err := toml.NewDecoder(f).Decode(&config); err != nil {
		log.Panicf("%s\n", err.Error())
	}

	return config
}
