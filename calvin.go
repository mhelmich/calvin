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
	"encoding/hex"
	"fmt"
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

	txnBatchChan := make(chan *pb.TransactionBatch)
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

	readyTxns := make(chan *pb.Transaction)
	doneTxnChan := make(chan *pb.Transaction)
	sched := scheduler.NewScheduler(txnBatchChan, readyTxns, doneTxnChan, srvr, logger)

	dataStore := newBoltDataStore(storeDir, logger)
	engine := execution.NewEngine(readyTxns, doneTxnChan, dataStore, srvr, cc, cip, logger)

	go srvr.Serve(lis)

	return &Calvin{
		cc:        cc,
		cip:       cip,
		seq:       seq,
		sched:     sched,
		engine:    engine,
		grpcSrvr:  srvr,
		dataStore: dataStore,
	}
}

type Calvin struct {
	cip       util.ClusterInfoProvider
	cc        util.ConnectionCache
	seq       *sequencer.Sequencer
	sched     *scheduler.Scheduler
	engine    *execution.Engine
	grpcSrvr  *grpc.Server
	dataStore *boltDataStore
}

func (c *Calvin) Stop() {
	c.grpcSrvr.Stop()
	c.seq.Stop()
	time.Sleep(time.Second)
	c.dataStore.close()
}

func (c *Calvin) SubmitTransaction(txn *pb.Transaction) {
	c.seq.SubmitTransaction(txn)
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
