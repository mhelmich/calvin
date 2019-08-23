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
	"html/template"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/ulid"
	"github.com/mhelmich/calvin/util"
	"github.com/stretchr/testify/assert"
)

func TestCalvinStartStop(t *testing.T) {
	configBags, ciPath := generateNConfigFiles(t, 1)
	configBag := configBags[0]
	c := NewCalvin(configBag.path, ciPath)
	assert.NotNil(t, c.cc)
	assert.NotNil(t, c.cip)
	c.Stop()
	defer os.RemoveAll(configBag.path)
	defer os.RemoveAll(fmt.Sprintf("./calvin-%d", configBag.id))
	defer os.RemoveAll(ciPath)
}

func TestCalvinPushTxns(t *testing.T) {
	configBags, ciPath := generateNConfigFiles(t, 1)
	configBag := configBags[0]
	c := NewCalvin(configBag.path, ciPath)

	for i := 0; i < 10; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)
		c.SubmitTransaction(&pb.Transaction{
			Id:              id.ToProto(),
			WriterNodes:     []uint64{configBag.id},
			ReadWriteSet:    [][]byte{[]byte("narf")},
			StoredProcedure: "__simple_setter__",
		})
	}

	time.Sleep(3 * time.Second)
	c.Stop()
	defer os.RemoveAll(configBag.path)
	defer os.RemoveAll(fmt.Sprintf("./calvin-%d", configBag.id))
	defer os.RemoveAll(ciPath)
}

func TestCalvinTwoNodes(t *testing.T) {
	configBags, ciPath := generateNConfigFiles(t, 2)
	c1 := NewCalvin(configBags[0].path, ciPath)
	c2 := NewCalvin(configBags[1].path, ciPath)

	// f1, err := os.Create("./cmd/calvin.pprof")
	// assert.Nil(t, err)
	// f2, err := os.Create("./cmd/calvin.mprof")
	// assert.Nil(t, err)
	// pprof.StartCPUProfile(f1)
	// pprof.WriteHeapProfile(f2)
	// defer f2.Close()

	for i := 0; i < 100; i++ {
		id, err := ulid.NewId()
		assert.Nil(t, err)

		arg1 := &pb.SimpleSetterArg{
			Key:   []byte("narf"),
			Value: []byte(fmt.Sprintf("narf_value_%d", i)),
		}
		argBites1, err1 := arg1.Marshal()
		assert.Nil(t, err1)

		arg2 := &pb.SimpleSetterArg{
			Key:   []byte("mrmoep"),
			Value: []byte(fmt.Sprintf("moep_value_%d", i)),
		}
		argBites2, err2 := arg2.Marshal()
		assert.Nil(t, err2)

		txn := &pb.Transaction{
			Id:          id.ToProto(),
			WriterNodes: []uint64{configBags[0].id, configBags[1].id},
			// I tested that one key each lands on each node
			ReadWriteSet:        [][]byte{[]byte("narf"), []byte("mrmoep")},
			StoredProcedure:     "__simple_setter__",
			StoredProcedureArgs: [][]byte{argBites1, argBites2},
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
	pprof.StopCPUProfile()

	c1.Stop()
	c2.Stop()

	defer os.RemoveAll(configBags[0].path)
	defer os.RemoveAll(configBags[1].path)
	defer os.RemoveAll(fmt.Sprintf("./calvin-%d", configBags[0].id))
	defer os.RemoveAll(fmt.Sprintf("./calvin-%d", configBags[1].id))
	defer os.RemoveAll(ciPath)
}

// TODO: convert this to the new way of getting config files
func __TestCalvinThreeNodes(t *testing.T) {
	os.RemoveAll("./cmd/calvin-1")
	os.RemoveAll("./cmd/calvin-2")
	os.RemoveAll("./cmd/calvin-3")

	c1 := NewCalvin("./cmd/config_1.toml", "./cmd/cluster_info.toml")
	c2 := NewCalvin("./cmd/config_2.toml", "./cmd/cluster_info.toml")

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

	c3 := NewCalvin("./cmd/config_3.toml", "./cmd/cluster_info.toml")

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

	os.RemoveAll("./cmd/calvin-1")
	os.RemoveAll("./cmd/calvin-2")
	os.RemoveAll("./cmd/calvin-3")
}

var templateConfig = `
#
#  Copyright 2019 Marco Helmich
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

raft_id = {{.MyRaftID}}
hostname = "localhost"
port = {{.Port}}
store_path = "./calvin-"
peers = [ {{.OtherRaftIDs}} ]
`

var templateClusterInfo = `
#
#  Copyright 2019 Marco Helmich
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

number_primaries = 2
number_partitions = 6

[nodes]
	{{range .Nodes}}
		[nodes.{{.ID}}]
		id = {{.ID}}
		hostname = "localhost"
		port = {{.Port}}
		partitions = [ {{join .Partitions ", "}} ]
	{{end}}
`

type templateInputConfig struct {
	MyRaftID     uint64
	OtherRaftIDs string
	Port         int
}

type templateInputClusterInfo struct {
	Nodes []templateInputNode
}

type templateInputNode struct {
	ID         uint64
	Port       int
	Partitions []string
}

type configBag struct {
	path string
	id   uint64
}

func TestGenerateTemplates(t *testing.T) {
	bags, ciPath := generateNConfigFiles(t, 3)
	assert.Equal(t, 3, len(bags))

	for idx := range bags {
		if _, err := os.Stat(bags[idx].path); os.IsNotExist(err) {
			assert.True(t, false)
		}

		defer os.Remove(bags[idx].path)
	}

	defer os.Remove(ciPath)
}

func generateNConfigFiles(t *testing.T, n int) ([]configBag, string) {
	ids := make([]uint64, n)
	for idx := range ids {
		ids[idx] = util.RandomRaftId()
	}

	ports := make([]int, n)
	ports[0] = 5433
	for i := 1; i < len(ports); i++ {
		ports[i] = ports[i-1] + 1
	}

	tis := make([]*templateInputConfig, n)
	for idx := range tis {
		lids := make([]uint64, n)
		copy(lids, ids)
		peerIDs := removeIdx(lids, idx)
		peerIDsStr := make([]string, 0)
		for idx := range peerIDs {
			peerIDsStr = append(peerIDsStr, util.Uint64ToString(peerIDs[idx]))
		}

		tis[idx] = &templateInputConfig{
			MyRaftID:     ids[idx],
			Port:         ports[idx],
			OtherRaftIDs: strings.Join(peerIDsStr, ", "),
		}
	}

	bags := make([]configBag, 0)
	tmp := template.Must(template.New("templateConfig").Parse(templateConfig))

	for idx := range tis {
		path := fmt.Sprintf("./config-%d.toml", tis[idx].MyRaftID)
		bag := configBag{
			path: path,
			id:   tis[idx].MyRaftID,
		}

		bags = append(bags, bag)
		f, err := os.Create(path)
		defer f.Close()
		assert.Nil(t, err)
		err = tmp.Execute(f, tis[idx])
		assert.Nil(t, err)
	}

	ci := templateInputClusterInfo{
		Nodes: make([]templateInputNode, n),
	}

	for idx := range ci.Nodes {
		ci.Nodes[idx] = templateInputNode{
			ID:         ids[idx],
			Port:       ports[idx],
			Partitions: []string{strconv.Itoa(idx * 3), strconv.Itoa((idx * 3) + 1), strconv.Itoa((idx * 3) + 2)},
		}
	}

	funcs := template.FuncMap{
		"join": strings.Join,
	}
	tmpCi := template.Must(template.New("templateClusterInfo").Funcs(funcs).Parse(templateClusterInfo))
	path := fmt.Sprintf("./cluster_info-%d.toml", util.RandomRaftId())
	f, err := os.Create(path)
	defer f.Close()
	assert.Nil(t, err)
	err = tmpCi.Execute(f, ci)
	assert.Nil(t, err)

	return bags, path
}

// order preserving remove idx ... is probably inefficient though :/
func removeIdx(ids []uint64, idx int) []uint64 {
	if idx == 0 {
		return ids[1:]
	} else if idx == len(ids)-1 {
		return ids[:len(ids)-1]
	}

	return append(ids[:idx], ids[idx+1:]...)
}
