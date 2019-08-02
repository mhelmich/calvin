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
	"testing"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/stretchr/testify/assert"
)

func TestRemoteReadServer(t *testing.T) {
	readyExecEnvChan := make(chan *txnExecEnvironment, 1)
	rrs := newRemoteReadServer(readyExecEnvChan)

	id, err := ulid.NewId()
	assert.Nil(t, err)
	req := &pb.RemoteReadRequest{
		TxnId:         id.ToProto(),
		TotalNumLocks: uint32(2),
		Keys:          [][]byte{[]byte("narf")},
		Values:        [][]byte{[]byte("narf")},
	}
	resp, err := rrs.RemoteRead(context.TODO(), req)
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "", resp.Error)

	req = &pb.RemoteReadRequest{
		TxnId:         id.ToProto(),
		TotalNumLocks: uint32(2),
		Keys:          [][]byte{[]byte("moep")},
		Values:        [][]byte{[]byte("moep")},
	}
	resp, err = rrs.RemoteRead(context.TODO(), req)
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "", resp.Error)

	execEnv := <-readyExecEnvChan
	assert.Equal(t, 0, execEnv.txnId.CompareTo(id))
	fmt.Printf("%s\n", execEnv.String())
}
