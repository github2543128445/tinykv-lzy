// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A)
	if storage == nil {
		log.Panicf("nil Storage try to newLog")
	}
	ret := &RaftLog{
		storage: storage,
	}

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	hardState, _, _ := storage.InitialState()
	ret.committed = hardState.Commit
	ret.applied = firstIndex - 1
	ret.stabled = lastIndex
	ret.entries, _ = storage.Entries(firstIndex, lastIndex+1)
	ret.pendingSnapshot = nil
	ret.dummyIndex = firstIndex
	return ret

}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	return
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// my func get [l,r]
func (l *RaftLog) AddEntries(es []*pb.Entry) uint64 {
	for _, i := range es {
		l.entries = append(l.entries, *i)
	}
	return l.LastIndex()
}

// 输入为绝对Index，转化为相对Index后返回数据
func (l *RaftLog) GetEntries(left uint64, right uint64) []pb.Entry {
	if right > l.LastIndex() {
		right = l.LastIndex()
	}
	if left > right {
		return []pb.Entry{} //mayBUG
	}
	start, end := left-l.dummyIndex, right-l.dummyIndex
	return l.entries[start : end+1]

}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.GetEntries(l.stabled+1, l.LastIndex())
}

// nextEnts returns all the committed but not applied entries

func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed > l.applied {
		return l.GetEntries(l.applied+1, l.committed)
	}
	return []pb.Entry{}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex - 1 + uint64(len(l.entries))
}
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) { //有可能是快照中的， 待快照的，其他的
	// Your Code Here (2A).
	if i > l.LastIndex() { //超区，不存在0的term，应该在外面就能检测出冲突来了MAYBUG
		return 0, nil
	}
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	//快照的TODO
	term, err := l.storage.Term(i)
	return term, err
}

func (l *RaftLog) DeleteFrom(i uint64) {
	if i < l.dummyIndex || len(l.entries) <= 0 {
		return
	}
	l.entries = l.entries[:i-l.dummyIndex]
}

// 被majority复制的log是否能提交
func (l *RaftLog) commitInRLog(commitId, term uint64) bool {
	commitTerm, _ := l.Term(commitId)
	if commitId > l.committed && commitTerm == term {
		// 只有日志索引大于当前的commitIndex
		// 当前任期内创建的日志才能提交（前面的也一并提交）
		l.committed = commitId
		return true
	}
	return false
}

// 比较来者是否拥有newer的数据
func (l *RaftLog) hasNewerData(id, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && id >= l.LastIndex())
}
