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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/stretchr/testify/assert"
)

func TestCalvinStartStop(t *testing.T) {
	os.RemoveAll("./calvin-1")
	c := NewCalvin("./config_1.toml", "./cluster_info.toml")
	assert.NotNil(t, c.cc)
	assert.NotNil(t, c.cip)
	c.Stop()
	os.RemoveAll("./calvin-1")
}

func TestCalvinPushTxns(t *testing.T) {
	os.RemoveAll("./calvin-1")
	c := NewCalvin("./config_1.toml", "./cluster_info.toml")

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

func TestCalvinTwoNodes(t *testing.T) {
	os.RemoveAll("./calvin-1")
	os.RemoveAll("./calvin-2")

	c1 := NewCalvin("./config_1.toml", "./cluster_info.toml")
	c2 := NewCalvin("./config_2.toml", "./cluster_info.toml")

	for i := 0; i < 100; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)
		txn := &pb.Transaction{
			Id:          id.ToProto(),
			WriterNodes: []uint64{1, 2},
			// I tested that one key each lands on each node
			ReadWriteSet:    [][]byte{[]byte("narf"), []byte("mrmoep")},
			StoredProcedure: "__simple_setter__",
			StoredProcedureArgs: [][]byte{
				[]byte(fmt.Sprintf("narf_value_%d", i)),
				[]byte(fmt.Sprintf("moep_value_%d", i)),
			},
		}

		if i%2 == 0 {
			c1.SubmitTransaction(txn)
		} else {
			c2.SubmitTransaction(txn)
		}

		if i%10 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	time.Sleep(3 * time.Second)

	c1.Stop()
	c2.Stop()

	os.RemoveAll("./calvin-1")
	os.RemoveAll("./calvin-2")
}

func TestCalvinThreeNodes(t *testing.T) {
	os.RemoveAll("./calvin-1")
	os.RemoveAll("./calvin-2")
	os.RemoveAll("./calvin-3")

	c1 := NewCalvin("./config_1.toml", "./cluster_info.toml")
	c2 := NewCalvin("./config_2.toml", "./cluster_info.toml")

	for i := 0; i < 100; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)
		txn := &pb.Transaction{
			Id:          id.ToProto(),
			WriterNodes: []uint64{1, 2},
			// I tested that one key each lands on each node
			ReadWriteSet:    [][]byte{[]byte("narf"), []byte("mrmoep")},
			StoredProcedure: "__simple_setter__",
			StoredProcedureArgs: [][]byte{
				[]byte(fmt.Sprintf("narf_value_%d", i)),
				[]byte(fmt.Sprintf("moep_value_%d", i)),
			},
		}

		if i%2 == 0 {
			c1.SubmitTransaction(txn)
		} else {
			c2.SubmitTransaction(txn)
		}

		if i%10 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	time.Sleep(3 * time.Second)

	c3 := NewCalvin("./config_3.toml", "./cluster_info.toml")

	for i := 0; i < 100; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)
		txn := &pb.Transaction{
			Id:          id.ToProto(),
			WriterNodes: []uint64{1, 2},
			// I tested that one key each lands on each node
			ReadWriteSet:    [][]byte{[]byte("narf"), []byte("mrmoep")},
			StoredProcedure: "__simple_setter__",
			StoredProcedureArgs: [][]byte{
				[]byte(fmt.Sprintf("narf_value_%d", i)),
				[]byte(fmt.Sprintf("moep_value_%d", i)),
			},
		}

		if i%3 == 0 {
			c1.SubmitTransaction(txn)
		} else if i%3 == 1 {
			c2.SubmitTransaction(txn)
		} else {
			c3.SubmitTransaction(txn)
		}

		if i%10 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	c1.Stop()

	c2.Stop()
	c3.Stop()

	os.RemoveAll("./calvin-1")
	os.RemoveAll("./calvin-2")
	os.RemoveAll("./calvin-3")
}
