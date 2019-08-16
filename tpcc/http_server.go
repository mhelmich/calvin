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
	"encoding/json"
	"fmt"
	"net/http"
	netpprof "net/http/pprof"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/mhelmich/calvin"
	log "github.com/sirupsen/logrus"
)

func startNewHttpServer(port int, c *calvin.Calvin, logger *log.Entry) *httpServer {
	router := mux.NewRouter().StrictSlash(true)
	srvr := &httpServer{
		Server: http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      router,
			WriteTimeout: time.Second * 60,
			ReadTimeout:  time.Second * 60,
			IdleTimeout:  time.Second * 60,
		},
		c:      c,
		logger: logger,
	}

	router.
		Methods("GET").
		Path("/printInitStore").
		HandlerFunc(srvr.printInitStore).
		Name("initStore")

	router.
		Methods("GET").
		Path("/initStore").
		HandlerFunc(srvr.initStore).
		Name("initStore")

	// drag in pprof endpoints
	router.
		Path("/debug/pprof/cmdline").
		HandlerFunc(netpprof.Cmdline)

	router.
		Path("/debug/pprof/profile").
		HandlerFunc(netpprof.Profile)

	router.
		Path("/debug/pprof/symbol").
		HandlerFunc(netpprof.Symbol)

	router.
		Path("/debug/pprof/trace").
		HandlerFunc(netpprof.Trace)

	// at last register the prefix
	// NB: the trailing slash ("/") is a concession
	// to how gorilla does routing
	// when you want to go to the pprof page, you need to add
	// the trailing slash to your URL
	router.
		PathPrefix("/debug/pprof/").
		HandlerFunc(netpprof.Index)

	go srvr.ListenAndServe()
	return srvr
}

type httpServer struct {
	http.Server
	c      *calvin.Calvin
	logger *log.Entry
}

func (s *httpServer) printInitStore(w http.ResponseWriter, r *http.Request) {
	txns := initDatastore()
	a := make([]string, 0)
	size := uint64(0)
	for idx := range txns {
		size += uint64(txns[idx].Size())
		a = append(a, txns[idx].String())
		a = append(a, fmt.Sprintf("txn size: %d", txns[idx].Size()))
	}
	a = append(a, "overall txn size: "+strconv.FormatUint(size, 10))
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a)
}

func (s *httpServer) initStore(w http.ResponseWriter, r *http.Request) {
	a := make([]string, 0)
	txns := initDatastore()
	s.logger.Infof("Done generating txns [%d]", len(txns))
	a = append(a, fmt.Sprintf("Done generating txns [%d]", len(txns)))
	size := uint64(0)
	// s.c.SubmitTransaction(txns[0])
	for idx := range txns {
		size += uint64(txns[idx].Size())
		s.c.SubmitTransaction(txns[idx])
	}
	s.logger.Infof("Done submitting txns [%d]", size)
	a = append(a, fmt.Sprintf("Done generating txns [%d]", size))
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a)
}
