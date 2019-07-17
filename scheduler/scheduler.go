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
	"fmt"

	"github.com/mhelmich/calvin/pb"
	log "github.com/sirupsen/logrus"
)

type SchedulerConfig struct {
	SequencerChanIn <-chan *pb.TransactionBatch
	Logger          *log.Entry
}

type scheduler struct {
	sequencerChanIn <-chan *pb.TransactionBatch
	logger          *log.Entry
}

func NewScheduler(config *SchedulerConfig) *scheduler {
	s := &scheduler{
		sequencerChanIn: config.SequencerChanIn,
		logger:          config.Logger,
	}

	go s.runSequencerLoop()
	return s
}

func (s *scheduler) runSequencerLoop() {
	for {
		select {
		case batch := <-s.sequencerChanIn:
			if batch == nil {
				s.logger.Warningf("Ending scheduler loop")
				return
			}
			fmt.Printf("%s", batch.String())
		}
	}
}

func (s *scheduler) RunLowIsolationReadRequest(req *pb.LowIsolationReadRequest) *pb.LowIsolationReadResponse {
	return nil
}
