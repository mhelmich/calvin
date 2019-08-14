/*
 * Copyright 2018 Marco Helmich
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

package util

import (
	"fmt"
	"hash/fnv"
	"os"

	"github.com/naoina/toml"
	log "github.com/sirupsen/logrus"
)

type ClusterInfoProvider interface {
	FindOwnerFor(key []byte) uint64
	IsLocal(key []byte) bool
	AmIWriter(writerNodes []uint64) bool
	GetAddressFor(nodeID uint64) string
}

func NewClusterInfoProvider(ownNodeID uint64, pathToClusterInfo string) ClusterInfoProvider {
	return &cip{
		ownNodeID: ownNodeID,
		ci:        readClusterInfo(pathToClusterInfo),
	}
}

type cip struct {
	ownNodeID uint64
	ci        clusterInfo
}

func (c *cip) hashKeyToPartition(key []byte) int {
	hasher := fnv.New64()
	hasher.Write(key)
	return int(hasher.Sum64() % uint64(c.ci.NumberPartitions))
}

func (c *cip) FindOwnerFor(key []byte) uint64 {
	partition := c.hashKeyToPartition(key)
	for nodeID, node := range c.ci.Nodes {
		for idx := range node.Partitions {
			if node.Partitions[idx] == partition {
				return nodeID
			}
		}
	}

	return uint64(0)
}

func (c *cip) IsLocal(key []byte) bool {
	partition := c.hashKeyToPartition(key)
	node := c.ci.Nodes[c.ownNodeID]
	for idx := range node.Partitions {
		if node.Partitions[idx] == partition {
			return true
		}
	}
	return false
}

func (c *cip) AmIWriter(writerNodes []uint64) bool {
	for idx := range writerNodes {
		if writerNodes[idx] == c.ownNodeID {
			return true
		}
	}
	return false
}

func (c *cip) GetAddressFor(nodeID uint64) string {
	node, ok := c.ci.Nodes[nodeID]
	if !ok {
		return ""
	}

	return fmt.Sprintf("%s:%d", node.Hostname, node.Port)
}

type clusterInfo struct {
	NumberPrimaries  int
	NumberPartitions int
	Nodes            map[uint64]node
}

type node struct {
	ID         uint64
	Hostname   string
	Port       int
	Partitions []int
}

func readClusterInfo(path string) clusterInfo {
	f, err := os.Open(path)
	if err != nil {
		log.Panicf("%s\n", err.Error())
	}
	defer f.Close()

	var ci clusterInfo
	if err := toml.NewDecoder(f).Decode(&ci); err != nil {
		log.Panicf("%s\n", err.Error())
	}

	return ci
}
