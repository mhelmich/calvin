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
	"fmt"

	"github.com/mhelmich/calvin/pb"
	glua "github.com/yuin/gopher-lua"
	gluar "layeh.com/gopher-luar"
)

var procs = map[string]string{
	"lua_": ``,
}

func runLua(txn *pb.Transaction, execEnv *txnExecEnvironment, store *luaDataStore) error {
	lua := glua.NewState()
	defer lua.Close()
	lua.SetGlobal("store", gluar.New(lua, store))
	script, ok := procs[txn.StoredProcedure]
	if !ok {
		return fmt.Errorf("Can't find proc [%s]", txn.StoredProcedure)
	}
	return lua.DoString(script)
}
