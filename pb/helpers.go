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

import fmt "fmt"

func (m *Transaction) AddSimpleSetterArg(key []byte, value []byte) error {
	if m.StoredProcedure == "" {
		m.StoredProcedure = "__simple_setter__"
	} else if m.StoredProcedure != "__simple_setter__" {
		return fmt.Errorf("Can't add simple setter arg to txn not calling '__simple_setter__'")
	}

	arg := &SimpleSetterArg{
		Key:   key,
		Value: value,
	}
	bites, err := arg.Marshal()
	if err != nil {
		return err
	}

	// the index for the key and value need to align
	// otherwise the lua script doesn't work :)
	m.ReadWriteSet = append(m.ReadWriteSet, key)
	m.StoredProcedureArgs = append(m.StoredProcedureArgs, bites)
	return nil
}
