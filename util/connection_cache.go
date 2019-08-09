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
	"sync"

	"github.com/mhelmich/calvin/pb"
	"google.golang.org/grpc"
)

func NewConnectionCache(clusterInfoPath string) *connCache {
	cc := &connCache{
		nodeIDToConn: &sync.Map{},
		ci:           readClusterInfo(clusterInfoPath),
	}

	return cc
}

type ConnectionCache interface {
	GetRemoteReadClient(nodeID uint64) (pb.RemoteReadClient, error)
	GetRaftTransportClient(nodeID uint64) (pb.RaftTransportClient, error)
	GetSchedulerClient(nodeID uint64) (pb.SchedulerClient, error)
	Close()
}

type connCache struct {
	nodeIDToConn *sync.Map
	ci           ClusterInfo
}

func (cc *connCache) GetRemoteReadClient(nodeID uint64) (pb.RemoteReadClient, error) {
	conn, err := cc.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	return pb.NewRemoteReadClient(conn), nil
}

func (cc *connCache) GetRaftTransportClient(nodeID uint64) (pb.RaftTransportClient, error) {
	conn, err := cc.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	return pb.NewRaftTransportClient(conn), nil
}

func (cc *connCache) GetSchedulerClient(nodeID uint64) (pb.SchedulerClient, error) {
	conn, err := cc.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	return pb.NewSchedulerClient(conn), nil
}

func (cc *connCache) getConn(nodeID uint64) (*grpc.ClientConn, error) {
	addr := cc.getAddressFor(nodeID)
	c, ok := cc.nodeIDToConn.Load(nodeID)

	if !ok {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		var loaded bool
		c, loaded = cc.nodeIDToConn.LoadOrStore(nodeID, conn)
		if loaded {
			defer conn.Close()
		}
	}

	return c.(*grpc.ClientConn), nil
}

func (cc *connCache) Close() {
	cc.nodeIDToConn.Range(func(key, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		defer conn.Close()
		return true
	})
}

func (cc *connCache) getAddressFor(nodeID uint64) string {
	info := staticClusterInfo.Nodes[nodeID]
	return fmt.Sprintf("%s:%d", info.Hostname, info.Port)
}