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
	"testing"
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/stretchr/testify/assert"
)

func TestCalvinStartStop(t *testing.T) {
	os.RemoveAll("./calvin-1")
	c := NewCalvin("./config.toml", "./cluster_info.toml")
	assert.NotNil(t, c.cc)
	assert.NotNil(t, c.cip)
	c.Stop()
	os.RemoveAll("./calvin-1")
}

func TestCalvinPushTxns(t *testing.T) {
	os.RemoveAll("./calvin-1")
	c := NewCalvin("./config.toml", "./cluster_info.toml")

	for i := 0; i < 10; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)
		c.SubmitTransaction(&pb.Transaction{
			Id:           id.ToProto(),
			WriterNodes:  []uint64{1},
			ReadWriteSet: [][]byte{[]byte("narf")},
		})
	}

	time.Sleep(3 * time.Second)
	c.Stop()
	os.RemoveAll("./calvin-1")
}
