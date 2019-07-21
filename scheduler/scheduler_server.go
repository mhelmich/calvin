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

package scheduler

import (
	"context"
	"io"

	"github.com/mhelmich/calvin/pb"
	log "github.com/sirupsen/logrus"
)

func NewServer(logger *log.Entry) *SchedulerServer {
	return newServerWithLog(logger, transactionLog)
}

func newServerWithLog(logger *log.Entry, log *txnLog) *SchedulerServer {
	return &SchedulerServer{
		log:    log,
		logger: logger,
	}
}

type SchedulerServer struct {
	log    *txnLog
	logger *log.Entry
}

func (ss *SchedulerServer) Schedule(stream pb.Scheduler_ScheduleServer) error {
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(
				&pb.SchedulerResponse{
					Error: io.EOF.Error(),
				},
			)
		}

		ss.log.AddBatch(batch)
	}
}

func (ss *SchedulerServer) LowIsolationRead(context.Context, *pb.LowIsolationReadRequest) (*pb.LowIsolationReadResponse, error) {
	return nil, nil
}
