package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"src/labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	ELECT_TIMEOUT_MIN = 300
	ELECT_TIMEOUT_MAX = 600
	HEATBEAT_TIMEOUT  = 150
	TIMEINTERVAL      = 50
	activeWindowWidth = ELECT_TIMEOUT_MAX * time.Millisecond
)

// func getElectTimeOut() int {
// 	interval := ELECT_TIMEOUT_MIN + rand.Intn(ELECT_TIMEOUT_MAX-ELECT_TIMEOUT_MIN)
// 	return interval
// }

// func getHeartBeatTimeOut() time.Duration {
// 	return HEATBEAT_TIMEOUT * time.Millisecond
// }

type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// electionTimer  *time.Timer
	// heartbeatTimer *time.Timer
	killChan chan struct{}
	applyCh  chan ApplyMsg

	electionTimeout time.Duration
	lastElection    time.Time

	// heartbeatTimeout time.Duration
	lastHeartbeat time.Time

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isLeader     bool
	currentTerm  int
	votedFor     int
	voteReceived int
	log          []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	lastAck     []time.Time
}

func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for i := range rf.peers {
		if i != rf.me && time.Since(rf.lastAck[i]) <= activeWindowWidth {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := ELECT_TIMEOUT_MIN + rand.Intn(ELECT_TIMEOUT_MAX-ELECT_TIMEOUT_MIN)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) pastHeartbeatTimeout() bool {
	return time.Since(rf.lastHeartbeat) > HEATBEAT_TIMEOUT*time.Millisecond
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf1(4, "persist - rf.me: %d, rf.cTerm: %d, rf.voteFor: %d, rf.log: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

func (rf *Raft) getRole() (role RaftRole) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.isLeader {
		role = Leader
	} else if rf.votedFor == rf.me {
		role = Candidate
	} else {
		role = Follower
	}

	return
}

func (rf *Raft) changeToFollower(term int, votedFor int) {
	rf.isLeader = false
	rf.currentTerm = term
	rf.votedFor = votedFor
	// rf.resetElectionTimer()
	// rf.persist()
	DPrintf1(5, "[%d] become follower, Term : %d, VoteFor: %d", rf.me, term, votedFor)
	// resetTimer(rf.electionTimer, getElectTimeOut())
	// if (votedFor != -1) {
	// rf.electTimeTick = 0
	// }
}

func (rf *Raft) changeToCandidate() {
	rf.isLeader = false
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteReceived = 1
	rf.resetElectionTimer()
	rf.persist()
	DPrintf1(5, "[%d] become candidate: Term = %d", rf.me, rf.currentTerm)
}

func (rf *Raft) changeToLeader() {
	// DPrintf1(5, "[%d] become leader", rf.me)
	rf.isLeader = true
	// rf.currentTerm++

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1
		}
		// } else {
		// 	rf.matchIndex[i] = 0
		// }
	}
	DPrintf1(5, "[%d] become Leader: Term = %d", rf.me, rf.currentTerm)
	// rf.persist()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

	// DPrintf1(4, "readPersist - rf.me: %d, rf.cTerm: %d, rf.voteFor: %d, rf.log: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term         int
	VotedGranted bool
}

type RequestAppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	// Your data here.
	Term   int
	Sucess bool
	XTerm  int // conflict term
	XIndex int // conflict index

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term > rf.currentTerm {
	// 	rf.changeToFollower(args.Term, -1)
	// }

	reply.VotedGranted = false
	reply.Term = rf.currentTerm

	if (args.Term < rf.currentTerm) {
		return
	}

	if (args.Term > rf.currentTerm) {
		rf.changeToFollower(args.Term, -1)
		rf.persist()
	}

	reply.Term = rf.currentTerm

	logOK := false
	myLastLogIdx := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIdx].Term
	logOK = args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIdx)

	if logOK {
		// DPrintf1(4, "RequestVote: [%d] changeToFollower!", rf.me)
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.Term = args.Term
		reply.VotedGranted = true
		DPrintf("[%d] granted vote to [%d]", rf.me, args.CandidateId)
	} else {
		DPrintf("[%d] don' t granted vote to [%d]", rf.me, args.CandidateId)
		reply.VotedGranted = false
	}
}

// func (rf *Raft) quorumActive() bool {
// 	activePeers := 1
// 	for i, tracker := range rf.peerTrackers {
// 		if i != rf.me && time.Since(tracker.lastAck) <= activeWindowWidth {
// 			activePeers++
// 		}
// 	}
// 	return 2*activePeers > len(rf.peers)
// }

func (rf *Raft) setCommitIdx(idx int) {
	rf.commitIndex = idx
}

func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Sucess = false
	reply.XIndex = -1
	reply.XTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
		rf.persist()
	}

    reply.Term = rf.currentTerm
	if rf.getRole() != Follower {
		return
	}  


	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Sucess, reply.XIndex, reply.XTerm = false, len(rf.log), -1
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		idx := args.PrevLogIndex - 1
		for idx > 0 && rf.log[idx] == rf.log[args.PrevLogIndex] {
			idx--
		}

		reply.Sucess, reply.XIndex, reply.XTerm = false, idx, rf.log[args.PrevLogIndex].Term
	} else {
		reply.Sucess = true
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		// if logChanged {
		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			rf.setCommitIdx(min(args.LeaderCommit, len(rf.log)-1))
			// rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
	}
}

func (rf *Raft) processVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.getRole() != Candidate || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		// DPrintf1(4, "processVoteReply: [%d] changeToFollower!", rf.me)
		rf.changeToFollower(reply.Term, -1)
		rf.persist()
		return
		// resetTimer(rf.electionTimer, getElectTimeOut())
		// stopTimer(rf.heartbeatTimer)
	}

	if reply.VotedGranted && reply.Term == rf.currentTerm {
		// DPrintf1(2, "[%d] Vote Received!", rf.me)
		rf.voteReceived++
		// rf.electTimeTick = 0
		if rf.voteReceived > len(rf.peers)/2 {
			// DPrintf("[%d] become leader of term[%d]", rf.me, rf.currentTerm)
			rf.changeToLeader()
			// rf.broadcastAppendEntries(rf.currentTerm)
			// rf.broadcastAppendEntries(true)
			// stopTimer(rf.electionTimer)
			// DPrintf1(4, "[%d] broadcast appendentries starts", rf.me)
			// go rf.startHeartbeat()
			// DPrintf1(4, "[%d] broadcast appendentries ends", rf.me)
			// resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
		}
	}
}

func (rf *Raft) processAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply, peersIdx int) {
	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term, -1)
		rf.persist()
		return
	}

	if rf.getRole() != Leader || rf.currentTerm != args.Term || rf.nextIndex[peersIdx] - 1 != args.PrevLogIndex {
		return
	}	

	if !reply.Sucess {
		if reply.XTerm != -1 || reply.XIndex != -1 {
			if reply.XTerm == -1 {
				rf.nextIndex[peersIdx] = reply.XIndex
			} else {
				conflictTermIndex := -1
				for index := reply.XIndex; index > 0; index-- {
					if rf.log[index].Term == reply.XTerm {
						conflictTermIndex = index
						break
					}
				}
				if conflictTermIndex != -1 {
					rf.nextIndex[peersIdx] = conflictTermIndex
				} else {
					rf.nextIndex[peersIdx] = reply.XIndex
				}
			}

			// rf.broadcastAppendEntries(rf.currentTerm)
		}
			// return true
			// go rf.sendAppendEntries(peersIdx)
	} else {
		rf.matchIndex[peersIdx] = max(rf.matchIndex[peersIdx], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[peersIdx] = rf.matchIndex[peersIdx] + 1
		// rf.nextIndex[peersIdx] += len(args.Entries)
		// DPrintf1(5, "Node %d: receive logresponse! next: %d, delta: %d, loglen: %d", peersIdx, rf.nextIndex[peersIdx], logcnt, len(rf.log))
		if rf.nextIndex[peersIdx] > len(rf.log) {
			rf.nextIndex[peersIdx] = len(rf.log)
			DPrintf1(5, "Node %d log overflow! next: %d, delta: %d, loglen: %d", peersIdx, rf.nextIndex[peersIdx], len(args.Entries), len(rf.log))
		}
		// rf.matchIndex[peersIdx] = rf.nextIndex[peersIdx] - 1

		// rf.nextIndex[peersIdx] = len(rf.log)
		// rf.matchIndex[peersIdx] = len(rf.log) - 1
		candiCommitIdx := len(rf.log) - 1
		// DPrintf1(3, "rf.nextIndex = %v", rf.nextIndex)
		// DPrintf1(3, "rf.matchIndex = %v", rf.matchIndex)
		// DPrintf1(3, "candiCommitIdx = %v, rf.commitIndex = %v", candiCommitIdx, rf.commitIndex)
		// committed := false
	outerloop:
		for candiCommitIdx > rf.commitIndex {
			count := 0
			if rf.log[candiCommitIdx].Term == rf.currentTerm {
				for i := 0; i < len(rf.matchIndex); i++ {
					// DPrintf1(3, "candiCommitIdx = %v, count = %v, i = %v, rf.matchIndex[i] = %v", candiCommitIdx, count, i, rf.matchIndex[i])
					if rf.matchIndex[i] >= candiCommitIdx {
						count++
						if count > len(rf.matchIndex)/2 {
							rf.setCommitIdx(candiCommitIdx)
							// committed = true
							break outerloop
						}
					}
					// DPrintf1(3, "count = %v", count)
				}
			}

			candiCommitIdx--
		}

		// if committed {
		// 	rf.broadcastAppendEntries(rf.currentTerm)
		// }

	}

	return 
}

func (rf *Raft) broadcastRequestVote() {
	// rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	v := rf.log[lastLogIndex]
	lastLogTerm := v.Term
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	// DPrintf1(3, "[%d] broadcastRequestVote: Term: %d", args.CandidateId, args.Term)
	// rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if rf.getRole() != Candidate {
			return
		}

		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.peers[server].Call("Raft.RequestVote", args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.lastAck[server] = time.Now()
				rf.processVoteReply(&args, reply)
			}
		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	// for {
	rf.mu.Lock()
	if rf.getRole() != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIdx := rf.nextIndex[server] - 1
	if prevLogIdx == -1 {
		DPrintf1(5, "sendAppendEntries:server = [%d], prevLogIdx = [%d]", server, prevLogIdx)
		rf.mu.Unlock()
		return
	}

	args := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  rf.log[prevLogIdx].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]LogEntry, 0),
	}

	// entryCnt := 0
	// for logIdx := rf.nextIndex[server]; logIdx < len(rf.log); logIdx++ {
	// 	args.Entries = append(args.Entries, rf.log[logIdx])
	// 	entryCnt++
	// 	// if entryCnt >= MAX_ENTRIES {
	// 	// 	break
	// }
	args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]:]...)
	// if len(args.Entries) != 0 && rf.nextIndex[server] == len(rf.log){
	// DPrintf1(5, "Node %d, Entries Len : %d", server, len(args.Entries))
	// }
	// }

	// if len(args.Entries) > 0 {
	// 	DPrintf1(5, "replicating log", "to whom",id, "arg", arg,"nextIndex",raft.NextIndex[id])
	// }

	rf.mu.Unlock()

	reply := &RequestAppendEntriesReply{}
	// DPrintf1(5, "[%d] Send RequestAppendEntriesArgs to [%d]: Term: %d, LeaderId: %d, LeaderCommit: %d", rf.me, server, args.Term, args.LeaderId, args.LeaderCommit)
	if !rf.peers[server].Call("Raft.RequestAppendEntries", args, reply) {
		return
	}

	rf.mu.Lock()
	rf.lastAck[server] = time.Now()
	rf.processAppendEntries(&args, reply, server)
	rf.mu.Unlock()
	// return
	// }
}

func (rf *Raft) hasNewEntries(to int) bool {
	return len(rf.log) >= rf.nextIndex[to]
}

func (rf *Raft) broadcastAppendEntries(forced bool) {
	// rf.mu.Lock()
	// rf.mu.Unlock()
	// rf.nextIndex[rf.me] = len(rf.log)
	// rf.matchIndex[rf.me] = len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if forced || rf.hasNewEntries(i) {
			go rf.sendAppendEntries(i)
		}
	}
}

func (rf *Raft) startNewElection() {
	rf.changeToCandidate()
	rf.broadcastRequestVote()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.isLeader
	if isLeader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		rf.persist()
		rf.broadcastAppendEntries(true)
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.killChan)
}

// func stopTimer(timer *time.Timer) {
// 	timer.Stop()
// 	select {
// 	case <-timer.C:
// 	default:
// 	}

// 	// rf.electionTimer.Reset(getElectTimeOut())
// }

// func resetTimer(timer *time.Timer, du time.Duration) {
// 	stopTimer(timer)
// 	timer.Reset(du)
// }

func (rf *Raft) ticker() {
	for {
		select {
			case <-rf.killChan:
				return
			default:
				time.Sleep(TIMEINTERVAL * time.Millisecond)
				rf.mu.Lock()
				
				switch rf.getRole() {
				case Follower:
					fallthrough
				case Candidate:
					if rf.pastElectionTimeout(){
						rf.startNewElection()
					}
				case Leader:
					if !rf.quorumActive() {
						rf.changeToFollower(rf.currentTerm, -1)
						break
					}
					forced := false
					if rf.pastHeartbeatTimeout() {
						forced = true
						rf.resetHeartbeatTimer()
					}
					
					rf.broadcastAppendEntries(forced)
				}
				rf.mu.Unlock()
		}
	}
}

// func (rf *Raft) startElectionTimer() {
// 	// resetTimer(rf.electionTimer, getElectTimeOut())
// 	for {
// 		select {
// 		// case <-rf.electionTimer.C:
// 		// 	rf.mu.Lock()
// 		// 	role := rf.getRole()
// 		// 	if role != Leader {
// 		// 		DPrintf1(4, "[%d] [%d] start new Election", rf.me, rf.getRole())
// 		// 		rf.startNewElection()
// 		// 	}

// 		// 	resetTimer(rf.electionTimer, getElectTimeOut())
// 		// 	rf.mu.Unlock()
// 		case <-rf.killChan:
// 			return
// 		default:
// 			time.Sleep(TIMEINTERVAL * time.Millisecond)
// 			rf.mu.Lock()
// 			if rf.getRole() != Leader {
// 				if rf.pastElectionTimeout() {
// 					rf.startNewElection()
// 				}
// 			}
// 			rf.mu.Unlock()
// 		}
// 	}
// }

// func (rf *Raft) startHeartbeat() {
// 	// DPrintf1(5, "%d start heartbeat", rf.me)
// 	// defer DPrintf1(5, "%d end heartbeat", rf.me)
// 	// rf.broadcastAppendEntries(true)
// 	// resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())

// 	for {
// 		select {
// 		case <-rf.killChan:
// 			return
// 		default:
// 			time.Sleep(TIMEINTERVAL * time.Millisecond)
// 			rf.mu.Lock()
// 			if rf.getRole() == Leader {
// 				if rf.pastHeartbeatTimeout() {
// 					term := rf.currentTerm
// 					rf.broadcastAppendEntries(term)
// 					rf.resetHeartbeatTimer()
// 				}
// 			}
// 			rf.mu.Unlock()
// 			// case <-rf.heartbeatTimer.C:
// 			// 	rf.mu.Lock()
// 			// 	role := rf.getRole()
// 			// 	if role == Leader {
// 			// 		// stopTimer(rf.heartbeatTimer)
// 			// 		rf.broadcastAppendEntries(rf.currentTerm)
// 			// 	}
// 			// 	resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
// 			// 	rf.mu.Unlock()
// 		}
// 	}
// }

func (rf *Raft) newCommittedEntries() []LogEntry {
	// FIXME: replace with `start == end` and verify it.
	if rf.commitIndex <= rf.lastApplied + 1 {
		// note: len(nil slice) == 0.
		return nil
	}
	cloned := make([]LogEntry, 0)
	cloned = append(cloned, rf.log[rf.lastApplied + 1: rf.commitIndex]...)
	return cloned
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.killChan:
			return
		default:
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied && rf.lastApplied < len(rf.log)-1 {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					Index:   rf.lastApplied,
					Command: rf.log[rf.lastApplied].Command,
				}

				DPrintf1(3, "[%d] applyMsg idx = [%d] :[%+v]", rf.me, rf.lastApplied, applyMsg)
				applyCh <- applyMsg
			}
			rf.mu.Unlock()
		}
	}	
	// for {
	// 	select {
	// 	case <-rf.killChan:
	// 		return
	// 	default:
	// 		time.Sleep(50 * time.Millisecond)
	// 		rf.mu.Lock()
	// 		if newCommittedEntries := rf.newCommittedEntries(); len(newCommittedEntries) > 0 {
	// 			lastApplied := rf.lastApplied
	// 			rf.mu.Unlock()

	// 			for _, entry := range newCommittedEntries {
	// 				lastApplied++
	// 				applyMsg := ApplyMsg{
	// 					Index:   lastApplied,
	// 					Command: entry.Command,
	// 				}

	// 				DPrintf1(3, "[%d] applyMsg idx = [%d] :[%+v]", rf.me, rf.lastApplied, applyMsg)
	// 				applyCh <- applyMsg
	// 			}

	// 			rf.mu.Lock()
	// 			rf.lastApplied = lastApplied
	// 		}
	// 		rf.mu.Unlock()
	// 	}
	// }
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: -1, Command: nil}) // index strat from 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAck = make([]time.Time, len(rf.peers))
	// rf.electionTimer = time.NewTimer(getElectTimeOut())
	// rf.heartbeatTimer = time.NewTimer(getHeartBeatTimeOut())
	rf.killChan = make(chan struct{})

	// rf.electTimeTick = 0
	// rf.heartbeatTick = 0
	// rf.electDuration = getElectTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf1(5, "Node %d: States = %d", rf.me, rf.getRole())

	rf.resetElectionTimer()
	// rf.heartbeatTimeout = heartbeatTimeout

	// go rf.startElectionTimer()
	// go rf.startHeartbeat()
	go rf.ticker()
	go rf.applyLogLoop(applyCh)

	return rf
}
