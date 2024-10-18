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
	"errors"
	"math"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是最后应用的索引。它应该仅在重新启动 raft 时设置。
	// raft 不会向应用程序返回小于或等于 Applied 的条目。
	// 如果在重新启动时未设置 Applied，raft 可能会返回之前应用的条目。这是一个非常依赖于应用程序的配置。
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Progress 表示在领导者看来追随者的进展。领导者维护所有追随者的进展，并根据其进展向追随者发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// 论文中提到了随机选举时间,实现上考虑增加字段大小为[0.8*eT,1.8*eT]
	randElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 一次只能有一个配置更改处于待处理状态（在日志中，但尚未应用）
	// 这通过 PendingConfIndex 来强制执行，它被设置为一个值，
	// 该值大于等于最新待处理配置更改（如果有）的日志索引。
	// 只有当领导者的应用索引大于此值时，才允许提议配置更改。
	PendingConfIndex uint64

	agreedCnt int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id: c.ID,
	}
	raftlog := newLog(c.Storage)
	hardstate, ConfState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = ConfState.Nodes
	}
	r.Prs = make(map[uint64]*Progress)
	for _, pr := range c.peers {
		r.Prs[pr] = &Progress{0, 0}
	}
	r.Term = hardstate.Term
	r.Vote = hardstate.Vote
	r.RaftLog = raftlog
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.msgs = nil
	r.Lead = None
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick
	r.resetRandElectionTimeout()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	toPg := r.Prs[to]
	prevLogIndex := toPg.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err == nil {
		appendMsg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: prevLogTerm,  //意味着发送的Entry的上一Entry的term
			Index:   prevLogIndex, //意味着发送的Entry的上一Index
			Entries: make([]*pb.Entry, 0),
			Commit:  r.RaftLog.committed,
		}
		entris := r.RaftLog.GetEntries(prevLogIndex+1, math.MaxUint64) //发的时候不发Index的
		for i := range entris {
			appendMsg.Entries = append(appendMsg.Entries, &entris[i])
		}
		r.msgs = append(r.msgs, appendMsg)
		//做好发送准备即可
		return true
	}
	//有可能需要发的已经变成快照了，这是之后的内容了
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, To: r.id, From: r.id})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
		}
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			//r.becomeCandidate() step_msgHup再变
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
		}
	}
}
func (r *Raft) resetRandElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandElectionTimeout() //mayBUG
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.agreedCnt = 0

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandElectionTimeout()
	r.agreedCnt = 1
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0 //之后要操作
	}
	// r.agreedCnt = 0
	// r.Vote = None
	// r.votes = make(map[uint64]bool)
	//mayBUG我认为不用管，因为leader不会直接变Candidate，只要经过Follower就会更新
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, To: r.id, From: r.id, Entries: []*pb.Entry{{}}})
	// NOTE: Leader should propose a noop entry on its term
}
func (r *Raft) leaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		return r.stepMsgBeat(m)
	case pb.MessageType_MsgPropose:
		return r.stepMsgPropose(m)
	case pb.MessageType_MsgAppend:
		return r.stepMsgAppend(m) //mayBUG领导也要管别人的entry吗
	case pb.MessageType_MsgAppendResponse:
		return r.stepMsgAppendResponse(m) //mayBUG可以顶下领导，为什么不用心跳做？
	case pb.MessageType_MsgRequestVote:
		return r.stepMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		//TODOnextProject
	case pb.MessageType_MsgHeartbeat:
		//mayBUG领导万一能听到别人的心跳呢
	case pb.MessageType_MsgHeartbeatResponse:
		return r.stepMsgHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:

	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}
func (r *Raft) candidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return r.stepMsgHup(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		return r.stepMsgAppend(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		return r.stepMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		return r.stepMsgRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:

	case pb.MessageType_MsgHeartbeat:
		return r.stepMsgHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:

	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}
func (r *Raft) followerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return r.stepMsgHup(m) //test有Follower发起选举
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		return r.stepMsgAppend(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		return r.stepMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:

	case pb.MessageType_MsgHeartbeat:
		return r.stepMsgHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:

	case pb.MessageType_MsgTransferLeader:

	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}
func (r *Raft) stepMsgHup(m pb.Message) error {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return nil
	}
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
	}
	return nil
}
func (r *Raft) stepMsgBeat(m pb.Message) error {
	if len(r.Prs) == 1 {
		return nil
	}
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	//在tick中清0了
	return nil
}
func (r *Raft) stepMsgPropose(m pb.Message) error {
	r.appendEntries(m.Entries)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
	return nil
}
func (r *Raft) stepMsgAppend(m pb.Message) error {
	r.handleAppendEntries(m)
	return nil
}
func (r *Raft) stepMsgAppendResponse(m pb.Message) error {
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return nil
	} else {
		if r.Prs[m.From].updateProgress(m.Index) {
			if r.checkCommit() {
				for id := range r.Prs { //mayBUG 是不是发的有点多了
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
			}
		}
		//3ATODO
		return nil
	}
}
func (r *Raft) stepMsgRequestVote(m pb.Message) error {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term > r.Term || (m.Term == r.Term && (r.Vote == m.From || r.Vote == None) && r.RaftLog.hasNewerData(m.Index, m.LogTerm)) {
		resp.Reject = false
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, resp)
	return nil
}

func (r *Raft) stepMsgRequestVoteResponse(m pb.Message) error {
	r.votes[m.From] = !m.Reject
	majority := len(r.Prs)/2 + 1
	if !m.Reject {
		r.agreedCnt++
	} else {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		} //mayBUG有newer的数据的不管吗
	}

	if r.agreedCnt >= majority {
		r.becomeLeader()
	} else {
		if len(r.votes)-r.agreedCnt >= majority {
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}
func (r *Raft) stepMsgSnapshot(m pb.Message) error {
	return nil
	//TODO nextProject
}
func (r *Raft) stepMsgHeartbeat(m pb.Message) error {
	r.handleHeartbeat(m)
	return nil
}
func (r *Raft) stepMsgHeartbeatResponse(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	} else {
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() { //MayBUG
			r.sendAppend(m.From)
		}
	}
	return nil

}
func (r *Raft) stepMsgTransferLeader(m pb.Message) error {
	//TODO nextProject
	return nil
}
func (r *Raft) stepMsgTimeoutNow(m pb.Message) error {
	//TODO nextProject
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.followerStep(m)
	case StateCandidate:
		return r.candidateStep(m)
	case StateLeader:
		return r.leaderStep(m)
	}
	return nil
}
func (r *Raft) appendEntries(es []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, e := range es {
		e.Index = lastIndex + uint64(i) + 1
		e.Term = r.Term
		if e.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = e.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		//Index是已经接受的合法Entry的最后一个Index
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, resp)
		return
	} //1任期冲突，发送者老了

	r.Term = m.Term
	r.Lead = m.From
	r.becomeFollower(m.Term, m.From) //状态更新

	rlogLastIndex := r.RaftLog.LastIndex()
	rlogTermAboutThisEntry, _ := r.RaftLog.Term(m.Index)
	if m.Index > rlogLastIndex || m.LogTerm != rlogTermAboutThisEntry { //2发早了，或者之前的数据任期对不上
		resp.Index = r.RaftLog.LastIndex() //发早了的处理

		if m.Index <= r.RaftLog.LastIndex() {
			conflictTerm, _ := r.RaftLog.Term(m.Index)
			for _, e := range r.RaftLog.entries {
				if e.Term == conflictTerm {
					resp.Index = e.Index - 1 //从前往后找，返回的是老任期log的Index
					break
				}
			}
		}

	} else {
		if len(m.Entries) > 0 { // mayBUG
			baseIndex := m.Index + 1
			var i uint64 = 0
			//检查前面有没有冲突
			for ; baseIndex+i < r.RaftLog.LastIndex() && baseIndex+i <= m.Entries[len(m.Entries)-1].Index; i++ { // < mayBUG
				term, _ := r.RaftLog.Term(baseIndex + i)
				if term != m.Entries[i].Term {
					break
				}
			}
			if i != uint64(len(m.Entries)) {
				r.RaftLog.DeleteFrom(i)
				r.RaftLog.AddEntries(m.Entries[i:])
				r.RaftLog.stabled = min(r.RaftLog.stabled, baseIndex+i-1) //截断可能导致已经持久化的部分减少到新Entry之前
			}
		}
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries))) //Commit是本节点中Commit的
		}
		resp.Reject = false
		resp.Index = m.Index + uint64(len(m.Entries))
		resp.LogTerm, _ = r.RaftLog.Term(resp.Index)
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
	}
	if r.Term > m.Term {
		resp.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, resp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// 该函数同时帮助更新Next和Match，返回值代表是否为有效更新
func (p *Progress) updateProgress(i uint64) bool {
	ret := false
	if p.Match < i {
		p.Match = i
		p.Next = i + 1
		ret = true
	}
	return ret
}

// 检查能否推进Commit，返回值true代表推进了
func (r *Raft) checkCommit() bool {
	matchArray := []uint64{}
	for _, p := range r.Prs {
		matchArray = append(matchArray, p.Match)
	}
	sort.Slice(matchArray, func(i, j int) bool {
		return matchArray[i] > matchArray[j]
	}) //降序
	majorMatch := matchArray[len(matchArray)/2+1-1]
	return r.RaftLog.commitInRLog(majorMatch, r.Term)
}
