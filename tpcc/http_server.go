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
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/mhelmich/calvin"
	"github.com/mhelmich/calvin/tpcc/pb"
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
		Path("/initStore").
		HandlerFunc(srvr.initStore).
		Name("initStore")

	router.
		Methods("GET").
		Path("/lowIsolationRead/{entityType}/{key}").
		HandlerFunc(srvr.lowIsolationRead).
		Name("lowIsolationRead")

	router.
		Methods("GET").
		Path("/calvinLogToJson").
		HandlerFunc(srvr.calvinLogToJson).
		Name("calvinLogToJson")

	router.
		Methods("GET").
		Path("/calvinLockChainToAscii").
		HandlerFunc(srvr.calvinLockChainToAscii).
		Name("calvinLockChainToAscii")

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

func (s *httpServer) initStore(w http.ResponseWriter, r *http.Request) {
	a := make([]string, 0)
	txns := initDatastore()
	s.logger.Infof("Done generating txns [%d]", len(txns))
	a = append(a, fmt.Sprintf("Done generating txns [%d]", len(txns)))

	size := uint64(0)
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

func (s *httpServer) lowIsolationRead(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vType := vars["entityType"]
	vKey := vars["key"]
	key := []byte(vKey)
	bites, err := s.c.LowIsolationRead(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	var buf proto.Message
	switch vType {
	case "warehouse":
		buf = &pb.Warehouse{}
	default:
		s.logger.Errorf("Can't find entity [%s]", vType)
		return
	}

	err = proto.Unmarshal(bites, buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	jpb := &jsonpb.Marshaler{Indent: "  "}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	jpb.Marshal(w, buf)
}

func (s *httpServer) calvinLogToJson(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	s.c.LogToJson(w)
}

func (s *httpServer) calvinLockChainToAscii(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	s.c.LockChainToAscii(w)
}
