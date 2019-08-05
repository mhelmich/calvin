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

	"github.com/stretchr/testify/assert"
)

func TestCalvinBasic(t *testing.T) {
	c := NewCalvin("./config.toml", "./cluster_info.toml")
	assert.NotNil(t, c.cc)
	assert.NotNil(t, c.cip)
	c.Stop()
	defer os.RemoveAll("./calvin-1")
}
