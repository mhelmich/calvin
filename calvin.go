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

	"github.com/naoina/toml"
)

type config struct {
	Servers struct {
		Self struct {
			NodeID     uint64
			Role       string
			Hostname   string
			Port       int
			Partitions []int
		}
		NumberPrimaries uint64
	}
}

func (c *config) String() string {
	return fmt.Sprintf("NodeID: %d Role: %s NumberPrimaries: %d", c.Servers.Self.NodeID, c.Servers.Self.Role, c.Servers.NumberPrimaries)
}

func NewCalvin(configPath string) {
	c := readConfig(configPath)
	fmt.Printf(c.String())
}

func readConfig(path string) config {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var config config
	if err := toml.NewDecoder(f).Decode(&config); err != nil {
		panic(err)
	}

	return config
}
