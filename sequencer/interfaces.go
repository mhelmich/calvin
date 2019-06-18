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
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Internal interface.
type store interface {
	raft.Storage
	saveConfigState(confState raftpb.ConfState) error
	saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error
	dropLogEntriesBeforeIndex(index uint64) error
	saveSnap(snap raftpb.Snapshot) error
	dropOldSnapshots(numberOfSnapshotsToKeep int) error
	close()
}

// Internal interface that is only used in CopyCat raft backend.
// It was introduced for mocking purposes.
type raftTransport interface {
	sendMessages(msgs []raftpb.Message) *messageSendingResults
}

type messageSendingResults struct {
	failedMessages            []raftpb.Message
	succeededSnapshotMessages []raftpb.Message
}
