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
	"sync"
	"testing"

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	glua "github.com/yuin/gopher-lua"
	gluar "layeh.com/gopher-luar"
)

func TestLuaExecutorGluar(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	lds := newStoredProcDataStore(mockStore, [][]byte{[]byte("hello")}, [][]byte{[]byte("Lua")}, mockCIP)

	lua.SetGlobal("store", gluar.New(lua, lds))
	script := `
	  print("Hello from Lua !")
    store:Set('hello','Lua')
	  print(store:Get('hello'))
    print("Hello from " .. store:Get('hello') .. "!")
	`

	err := lua.DoString(script)
	assert.Nil(t, err)
	v := lds.Get("hello")
	assert.NotNil(t, v)
	assert.Equal(t, "Lua", v)
}

func TestLuaExecutorCallingLuaFromGoOldSchool(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	double := `
    function double(x)
      return x * 2;
    end
  `

	err := lua.DoString(double)
	assert.Nil(t, err)

	if err := lua.CallByParam(glua.P{
		Fn:      lua.GetGlobal("double"),
		NRet:    1,
		Protect: true,
	}, glua.LNumber(10)); err != nil {
		panic(err)
	}
	ret := lua.Get(-1) // returned value
	lua.Pop(1)         // remove received value
	if n, ok := ret.(glua.LNumber); ok {
		assert.Equal(t, float64(20), float64(n))
	}
}

func TestLuaExecutorCallingLuaFromGoGluar(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	double := `
    function double(x)
      return x * 2;
    end
  `

	err := lua.DoString(double)
	assert.Nil(t, err)

	if err := lua.CallByParam(glua.P{
		Fn:      lua.GetGlobal("double"),
		NRet:    1,
		Protect: true,
	}, gluar.New(lua, 10)); err != nil {
		panic(err)
	}
	ret := lua.Get(-1) // returned value
	lua.Pop(1)         // remove received value
	if n, ok := ret.(glua.LNumber); ok {
		assert.Equal(t, float64(20), float64(n))
	}
}

func TestLuaExecutorFancy(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	store := newStoredProcDataStore(mockStore, [][]byte{[]byte("moep"), []byte("narf")}, [][]byte{[]byte("moep_value"), []byte("narf_value")}, mockCIP)

	keys := []string{"narf", "moep"}
	args := []string{"narf_value", "moep_value"}
	lua.SetGlobal("store", gluar.New(lua, store))
	lua.SetGlobal("KEYC", gluar.New(lua, len(keys)))
	lua.SetGlobal("KEYV", gluar.New(lua, keys))
	lua.SetGlobal("ARGC", gluar.New(lua, len(args)))
	lua.SetGlobal("ARGV", gluar.New(lua, args))

	script := `
    print("Hello from Lua !")
    for i = 1, KEYC
    do
      print(KEYV[i])
    end
    for i = 1, ARGC
    do
      print(ARGV[i])
    end

    for i = 1, KEYC
    do
      store:Set(KEYV[i], ARGV[i])
    end
  `
	err := lua.DoString(script)
	assert.Nil(t, err)

	v := store.Get("narf")
	assert.Equal(t, "narf_value", v)
	v = store.Get("moep")
	assert.Equal(t, "moep_value", v)
}

func TestLuaExecutorScriptInvocation(t *testing.T) {
	arg1 := &pb.SimpleSetterArg{
		Key:   []byte("narf"),
		Value: []byte("narf_value"),
	}
	argBites1, err1 := arg1.Marshal()
	assert.Nil(t, err1)

	arg2 := &pb.SimpleSetterArg{
		Key:   []byte("moep"),
		Value: []byte("moep_value"),
	}
	argBites2, err2 := arg2.Marshal()
	assert.Nil(t, err2)

	txn := &pb.Transaction{
		StoredProcedure:     simpleSetterProcName,
		StoredProcedureArgs: [][]byte{argBites1, argBites2},
	}

	execEnv := &txnExecEnvironment{
		keys:   [][]byte{[]byte("narf"), []byte("moep")},
		values: [][]byte{[]byte("narf_value"), []byte("moep_value")},
	}

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	lds := newStoredProcDataStore(mockStore, execEnv.keys, execEnv.values, mockCIP)

	procs := &sync.Map{}
	procs.Store(simpleSetterProcName, simpleSetterProc)

	w := &worker{
		storedProcs:         procs,
		luaState:            glua.NewState(),
		compiledStoredProcs: make(map[string]*glua.LFunction),
	}
	w.runLua(txn, execEnv, lds)

	v := lds.Get("narf")
	assert.Equal(t, "narf_value", v)
	v = lds.Get("moep")
	assert.Equal(t, "moep_value", v)
}

func TestLuaExecutorProtoBufArg(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	store := newStoredProcDataStore(mockStore, [][]byte{[]byte("moep"), []byte("narf")}, [][]byte{[]byte("moep_value"), []byte("narf_value")}, mockCIP)

	keys := []string{"narf"}
	args := []*pb.SimpleSetterArg{
		&pb.SimpleSetterArg{
			Key:   []byte("narf"),
			Value: []byte("narf_value"),
		},
	}
	lua.SetGlobal("store", gluar.New(lua, store))
	lua.SetGlobal("KEYC", gluar.New(lua, len(keys)))
	lua.SetGlobal("KEYV", gluar.New(lua, keys))
	lua.SetGlobal("ARGC", gluar.New(lua, len(args)))
	lua.SetGlobal("ARGV", gluar.New(lua, args))

	script := `
    print("Hello from Lua !")
    for i = 1, KEYC
    do
      print(KEYV[i])
    end
    for i = 1, ARGC
    do
      print(ARGV[i].Key, ARGV[i].Value)
    end
		for i = 1, ARGC
    do
      store:Set(ARGV[i].Key, ARGV[i].Value)
			print(ARGV[i].Key, ARGV[i].Value)
    end
  `
	err := lua.DoString(script)
	assert.Nil(t, err)
}

func TestLuaSubsequentExecutions(t *testing.T) {
	ls := glua.NewState()
	defer ls.Close()

	script := `
	for i = 1, ARGC
	do
		--print(ARGV[i].Key, ARGV[i].Value)
		store:Set(ARGV[i].Key, ARGV[i].Value)
	end
	`

	fn, err := ls.LoadString(script)
	assert.Nil(t, err)

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	store := newStoredProcDataStore(
		mockStore,
		[][]byte{[]byte("key1"), []byte("key2")},
		[][]byte{[]byte("value1"), []byte("value2")},
		mockCIP)

	args1 := []ssa{
		ssa{
			Key:   "key1",
			Value: "value1",
		},
	}

	args2 := []ssa{
		ssa{
			Key:   "key2",
			Value: "value2",
		},
	}

	ls.SetGlobal("store", gluar.New(ls, store))
	ls.SetGlobal("ARGC", gluar.New(ls, len(args1)))
	ls.SetGlobal("ARGV", gluar.New(ls, args1))

	ls.Push(fn)
	err = ls.PCall(0, glua.MultRet, nil)
	assert.Nil(t, err)

	v1 := store.Get("key1")

	ls.SetGlobal("store", gluar.New(ls, store))
	ls.SetGlobal("ARGC", gluar.New(ls, len(args2)))
	ls.SetGlobal("ARGV", gluar.New(ls, args2))

	ls.Push(fn)
	err = ls.PCall(0, glua.MultRet, nil)
	assert.Nil(t, err)

	assert.Equal(t, "value1", v1)
	v2 := store.Get("key2")
	assert.Equal(t, "value2", v2)
}

func BenchmarkLuaExecutorScriptInvocation(b *testing.B) {
	txn := &pb.Transaction{
		StoredProcedure:     simpleSetterProcName,
		StoredProcedureArgs: [][]byte{[]byte("narf_arg"), []byte("moep_arg")},
	}

	execEnv := &txnExecEnvironment{
		keys:   [][]byte{[]byte("narf"), []byte("moep")},
		values: [][]byte{nil, nil},
	}

	mockCIP := new(mocks.ClusterInfoProvider)
	mockCIP.On("IsLocal", mock.AnythingOfType("[]uint8")).Return(true)
	mockCIP.On("FindPartitionForKey", mock.AnythingOfType("[]uint8")).Return(1)

	mockTxn := new(mocks.DataStoreTxn)
	mockTxn.On("Set", mock.AnythingOfType("[]uint8"), mock.AnythingOfType("[]uint8")).Return(nil)
	mockTxnProvider := new(mocks.DataStoreTxnProvider)
	mockTxnProvider.On("StartTxn", true).Return(mockTxn, nil)
	mockStore := new(mocks.PartitionedDataStore)
	mockStore.On("GetPartition", mock.AnythingOfType("int")).Return(mockTxnProvider, nil)
	lds := newStoredProcDataStore(mockStore, execEnv.keys, execEnv.values, mockCIP)

	procs := &sync.Map{}
	procs.Store(simpleSetterProcName, simpleSetterProc)

	w := &worker{
		storedProcs:         procs,
		luaState:            glua.NewState(),
		compiledStoredProcs: make(map[string]*glua.LFunction),
	}

	// f1, err := os.Create("./narf.pprof")
	// assert.Nil(b, err)
	// f2, err := os.Create("./narf.mprof")
	// assert.Nil(b, err)
	// pprof.StartCPUProfile(f1)
	// defer pprof.StopCPUProfile()
	// pprof.WriteHeapProfile(f2)
	// defer f2.Close()

	for i := 0; i < b.N; i++ {
		w.runLua(txn, execEnv, lds)
	}

	v := lds.Get("narf")
	assert.Equal(b, "narf_arg", v)
	v = lds.Get("moep")
	assert.Equal(b, "moep_arg", v)
}
