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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestClusterInfoIsLocal(t *testing.T) {
	key1 := "narf0"
	cip1 := NewClusterInfoProvider(uint64(1), "../tpcc/cluster_info.toml")
	isLocal := cip1.IsLocal([]byte(key1))
	log.Infof("'%s' is local on 1: %t", key1, isLocal)
	assert.True(t, isLocal)
	key2 := "mrmoep"
	cip2 := NewClusterInfoProvider(uint64(2), "../tpcc/cluster_info.toml")
	isLocal = cip2.IsLocal([]byte(key2))
	log.Infof("'%s' is local on 2: %t", key2, isLocal)
	assert.True(t, isLocal)
}

func TestClusterInfoFindOwnerAndLeaderForPartition(t *testing.T) {
	cip1 := NewClusterInfoProvider(uint64(1), "../tpcc/cluster_info.toml")
	cip3 := NewClusterInfoProvider(uint64(3), "../tpcc/cluster_info.toml")

	nodeID := cip3.FindOwnerForPartition(1)
	assert.Equal(t, uint64(2), nodeID)

	nodeID = cip1.FindOwnerForPartition(1)
	assert.Equal(t, uint64(2), nodeID)
}
