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
	"io"
	"testing"

	"github.com/mhelmich/calvin/mocks"
	"github.com/mhelmich/calvin/pb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBasic(t *testing.T) {
	idx := -1
	batches := make([]*pb.TransactionBatch, 2)
	batches[0] = &pb.TransactionBatch{
		Epoch: uint64(1),
	}
	batches[1] = &pb.TransactionBatch{}
	errors := make([]error, 2)
	errors[0] = nil
	errors[1] = io.EOF

	mockSS := new(mocks.Scheduler_ScheduleServer)
	mockSS.On("Recv").Return(
		func() *pb.TransactionBatch { idx++; return batches[idx] },
		func() error { return errors[idx] },
	)
	mockSS.On("SendAndClose", mock.AnythingOfType("*pb.SchedulerResponse")).Return(nil)

	log := &txnLog{
		m: make(map[uint64]*pb.TransactionBatch),
	}
	logger := logrus.WithFields(logrus.Fields{
		"component": "scheduler",
	})

	ss := newServerWithLog(logger, log)

	resp := ss.Schedule(mockSS)
	assert.Nil(t, resp)
	assert.Equal(t, 1, len(log.m))
	b := log.GetIndex(uint64(1))
	assert.NotNil(t, b)
	assert.Equal(t, uint64(1), b.Epoch)
	b = log.GetIndex(uint64(11))
	assert.Nil(t, b)
}
