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

package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/mhelmich/calvin"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfgFile := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Panicf("'%s' is not a number", os.Args[2])
	}

	log.SetLevel(log.DebugLevel)

	c := calvin.NewCalvin(cfgFile, "./cluster_info.toml")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	logger := log.WithFields(log.Fields{})
	startNewHttpServer(port, c, logger)

	<-sig
	logger.Warningf("Shutting down...\n")
	c.Stop()
}
