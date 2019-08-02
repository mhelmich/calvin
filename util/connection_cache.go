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
	"sync"

	"github.com/mhelmich/calvin/pb"
	"google.golang.org/grpc"
)

func NewConnectionCache() *connCache {
	cc := &connCache{
		nodeIDToConn: &sync.Map{},
	}

	cc.readClusterInfo("")
	return cc
}

type ConnectionCache interface {
	GetRemoteReadClient(nodeID uint64) (pb.RemoteReadClient, error)
	Close()
}

type connCache struct {
	nodeIDToConn *sync.Map
}

func (cc *connCache) GetRemoteReadClient(nodeID uint64) (pb.RemoteReadClient, error) {
	conn, err := cc.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	return pb.NewRemoteReadClient(conn), nil
}

func (cc *connCache) getConn(nodeID uint64) (*grpc.ClientConn, error) {
	addr := cc.getAddressForNodeID(nodeID)
	c, ok := cc.nodeIDToConn.Load(nodeID)

	if !ok {
		conn, err := grpc.Dial(addr)
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

func (cc *connCache) getAddressForNodeID(id uint64) string {
	return ""
}

func (cc *connCache) readClusterInfo(path string) {
	readClusterInfo(path)
}
