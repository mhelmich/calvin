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

	"github.com/mhelmich/calvin/util"
	"github.com/naoina/toml"
	log "github.com/sirupsen/logrus"
)

type config struct {
	RaftID   uint64
	Hostname string
	Port     int
}

func NewCalvin(configPath string, clusterInfoPath string) *calvin {
	cfg := readConfig(configPath)
	return &calvin{
		cc:  util.NewConnectionCache(clusterInfoPath),
		cip: util.NewClusterInfoProvider(cfg.RaftID, clusterInfoPath),
	}
}

type calvin struct {
	cip util.ClusterInfoProvider
	cc  util.ConnectionCache
}

func readConfig(path string) config {
	f, err := os.Open(path)
	if err != nil {
		log.Panicf("%s\n", err.Error())
	}
	defer f.Close()

	var config config
	if err := toml.NewDecoder(f).Decode(&config); err != nil {
		log.Panicf("%s\n", err.Error())
	}

	return config
}
