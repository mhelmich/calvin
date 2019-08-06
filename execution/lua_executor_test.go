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
	"testing"

	"github.com/stretchr/testify/assert"
	glua "github.com/yuin/gopher-lua"
	gluar "layeh.com/gopher-luar"
)

func TestLuaExecutorRegister(t *testing.T) {
	lua := glua.NewState()
	defer lua.Close()

	lds := &LuaDataStore{
		ds: &mapDataStore{
			m: make(map[string]string),
		},
	}

	lua.SetGlobal("ds", gluar.New(lua, lds))
	script := `
	  print("Hello from Lua !")
    ds:Set('hello','Lua')
	  print(ds:Get('hello'))
    print("Hello from " .. ds:Get('hello') .. "!")
	`

	err := lua.DoString(script)
	assert.Nil(t, err)
	v := lds.Get("hello")
	assert.NotNil(t, v)
	assert.Equal(t, "Lua", v)
}

type mapDataStore struct {
	m map[string]string
}

func (ds *mapDataStore) Get(key []byte) []byte {
	k := string(key)
	v := ds.m[k]
	return []byte(v)
}

func (ds *mapDataStore) Set(key []byte, value []byte) {
	k := string(key)
	v := string(value)
	ds.m[k] = v
}
