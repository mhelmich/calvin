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
)

func TestIsLocal(t *testing.T) {
	key1 := "narf"
	cip1 := NewClusterInfoProvider(uint64(1), "../cmd/cluster_info.toml")
	log.Infof("'%s' is local on 1: %t", key1, cip1.IsLocal([]byte(key1)))
	key2 := "mrmoep"
	cip2 := NewClusterInfoProvider(uint64(2), "../cmd/cluster_info.toml")
	log.Infof("'%s' is local on 2: %t", key2, cip2.IsLocal([]byte(key2)))
}
