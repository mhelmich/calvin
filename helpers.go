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
	"os"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/sequencer"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
	"github.com/naoina/toml"
	log "github.com/sirupsen/logrus"
)

func NewTransaction() *pb.Transaction {
	id, err := ulid.NewId()
	if err != nil {
		log.Panicf("Can't generate new ulid")
	}

	return &pb.Transaction{
		Id: id.ToProto(),
	}
}

func defaultOptionsWithFilePaths(configPath string, clusterInfoPath string) Options {
	cfg := readConfig(configPath)
	cip := util.NewClusterInfoProvider(cfg.RaftID, clusterInfoPath)
	return Options{
		hostname:            cfg.Hostname,
		port:                cfg.Port,
		raftID:              cfg.RaftID,
		peers:               cfg.Peers,
		clusterInfoProvider: cip,
	}
}

func DefaultOptions(partitionedDataStore util.PartitionedDataStore, clusterInfoProvider util.ClusterInfoProvider) Options {
	return Options{
		hostname:             "localhost",
		port:                 5432,
		raftID:               util.RandomRaftId(),
		clusterInfoProvider:  clusterInfoProvider,
		partitionedDataStore: partitionedDataStore,
	}
}

type Options struct {
	hostname             string
	port                 int
	storePath            string
	raftID               uint64
	peers                []uint64
	clusterInfoProvider  util.ClusterInfoProvider
	snapshotHandler      sequencer.SnapshotHandler
	partitionedDataStore util.PartitionedDataStore
	numWorkers           int
}

func (o Options) WithSnapshotHandler(snapshotHandler sequencer.SnapshotHandler) Options {
	o.snapshotHandler = snapshotHandler
	return o
}

func (o Options) WithDataStore(dataStore util.PartitionedDataStore) Options {
	o.partitionedDataStore = dataStore
	return o
}

func (o Options) WithPort(port int) Options {
	o.port = port
	return o
}

func (o Options) WithRaftID(raftID uint64) Options {
	o.raftID = raftID
	return o
}

func (o Options) WithNumWorkers(numWorkers int) Options {
	o.numWorkers = numWorkers
	return o
}

func (o Options) WithPeers(peers []uint64) Options {
	o.peers = peers
	return o
}

func (o Options) WithStorePath(storePath string) Options {
	o.storePath = storePath
	return o
}

type config struct {
	RaftID    uint64
	Hostname  string
	Port      int
	StorePath string
	Peers     []uint64
}

func readConfig(configPath string) config {
	f, err := os.Open(configPath)
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
