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

package sequencer

import (
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/mhelmich/calvin/interfaces"
	"github.com/mhelmich/calvin/mocks"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSequencerBasic(t *testing.T) {
	newRaftID := randomRaftId()
	logger := log.WithFields(log.Fields{
		"component": "blot_store",
		"raftIdHex": hex.EncodeToString(uint64ToBytes(newRaftID)),
		"raftId":    uint64ToString(newRaftID),
	})

	storeDir := "./raft-" + uint64ToString(newRaftID) + "/"
	// startFromExistingState := storageExists(storeDir)
	bs, err := openBoltStorage(storeDir, logger)
	assert.Nil(t, err)

	defer os.RemoveAll(storeDir)

	mockClient := new(mocks.RaftMessageClient)
	mockClient.On("SendMessages", mock.Anything).Return(&interfaces.RaftMessageSendingResults{})

	config := SequencerConfig{
		newRaftID:         newRaftID,
		localRaftStore:    bs,
		raftMessageClient: mockClient,
	}

	writerTxns, readerTxns, err := NewSequencer(config)
	assert.Nil(t, err)

	time.Sleep(1000 * time.Millisecond)

	close(readerTxns)
	close(writerTxns)
}
