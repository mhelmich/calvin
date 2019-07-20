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
	"net"

	"github.com/mhelmich/calvin/pb"
	"github.com/mhelmich/calvin/scheduler"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type server struct {
	grpcServer *grpc.Server
	logger     *log.Entry
}

func newServer(hostname string, port int, logger *log.Entry) {
	myAddress := fmt.Sprintf("%s:%d", hostname, port)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		logger.Panicf("failed to listen: %v", err)
	}

	s := &server{
		grpcServer: grpc.NewServer(),
		logger:     logger,
	}

	pb.RegisterSchedulerServer(s.grpcServer, scheduler.NewServer())
	go s.grpcServer.Serve(lis)
}
