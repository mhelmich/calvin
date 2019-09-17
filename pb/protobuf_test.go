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

package pb

import (
	fmt "fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageTypesPartialUnmarshal(t *testing.T) {
	txn := &Transaction{
		Type:        TRANSACTION,
		Id:          &Id128{Upper: 123, Lower: 456},
		ReaderNodes: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	data, err := txn.Marshal()
	assert.Nil(t, err)

	baseMsg := &BaseMessage{}
	err = baseMsg.Unmarshal(data)
	assert.Nil(t, err)
	assert.Equal(t, TRANSACTION, baseMsg.Type)

	txn2 := &Transaction{
		Type: baseMsg.Type,
	}
	err = txn2.Unmarshal(baseMsg.XXX_unrecognized)
	assert.Nil(t, err)
	assert.Equal(t, TRANSACTION, txn2.Type)
	fmt.Printf("%s\n", txn2.String())

	lowIsoRead := &LowIsoRead{
		Type: LOW_ISO_READ,
		LowIsolationReadResponse: &LowIsolationReadResponse{
			Term:  3,
			Index: 99,
		},
	}
	data, err = lowIsoRead.Marshal()
	assert.Nil(t, err)

	baseMsg = &BaseMessage{}
	err = baseMsg.Unmarshal(data)
	assert.Nil(t, err)
	assert.Equal(t, LOW_ISO_READ, baseMsg.Type)

	lowIsoRead2 := &LowIsoRead{
		Type: baseMsg.Type,
	}
	err = lowIsoRead2.Unmarshal(baseMsg.XXX_unrecognized)
	assert.Nil(t, err)
	assert.Equal(t, LOW_ISO_READ, lowIsoRead2.Type)
	fmt.Printf("%s\n", lowIsoRead2.String())
}
