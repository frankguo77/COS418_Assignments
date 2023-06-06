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
	ELECT_TIMEOUT_MIN = 150
	ELECT_TIMEOUT_MAX = 300
	HEATBEAT_TIMEOUT  = 100
	MAX_ENTRIES       = 20
)

func getElectTimeOut() time.Duration {
	interval := ELECT_TIMEOUT_MIN + rand.Intn(ELECT_TIMEOUT_MAX-ELECT_TIMEOUT_MIN)
	return time.Duration(interval) * time.Millisecond
}

func getHeartBeatTimeOut() time.Duration {
	return HEATBEAT_TIMEOUT * time.Millisecond
}

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
	mu             sync.Mutex
	peers          []*labrpc.ClientEnd
	persister      *Persister
	me             int // index into peers[]
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	killChan       chan struct{}
	applyCh        chan ApplyMsg

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
	rf.persist()
	DPrintf1(4, "[%d] become follower, Term : %d", rf.me, term)
}

func (rf *Raft) changeToCandidate() {
	rf.isLeader = false
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteReceived = 1
	rf.persist()
}

func (rf *Raft) changeToLeader() {
	DPrintf1(5, "[%d] become leader", rf.me)
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
	rf.persist()
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
	// RequestedIndex  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if args.Term > rf.currentTerm {
	// 	rf.changeToFollower(args.Term, -1)
	// }

	termOK := (args.Term > rf.currentTerm) || (args.Term == rf.currentTerm && rf.votedFor == -1)
	logOK := false

	if termOK {
		myLastLogIdx := len(rf.log) - 1
		myLastLogTerm := rf.log[myLastLogIdx].Term
		logOK = args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIdx)
	}

	

	if termOK && logOK {
		// DPrintf1(4, "RequestVote: [%d] changeToFollower!", rf.me)
		rf.changeToFollower(args.Term, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VotedGranted = true
		DPrintf("[%d] granted vote to [%d]", rf.me, args.CandidateId)
		resetTimer(rf.electionTimer, getElectTimeOut())
		DPrintf("[%d] timer reseted", rf.me)
	} else {
		DPrintf("[%d] don' t granted vote to [%d]", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
	}
}

func (rf *Raft) setCommitIdx(idx int) {

	rf.commitIndex = idx
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied].Command,
			}

			DPrintf1(3, "[%d] applyMsg idx = [%d] :[%+v]", rf.me, rf.lastApplied, applyMsg)
			rf.applyCh <- applyMsg
		}
	}()
}

func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}

	// DPrintf1(4,"[%d] received RequestAppendEntries from [%d],  mylongs is %d", rf.me, args.LeaderId,len(rf.log))

	termOK := args.Term == rf.currentTerm

	if termOK {
		resetTimer(rf.electionTimer, getElectTimeOut())
		// DPrintf1(4, "[%d] timer reseted", rf.me)
	}

	logOK := len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm

	if termOK && logOK {
		reply.Sucess = true
		reply.Term = args.Term
		// DPrintf1(2,"[%d] received RequestAppendEntries from [%d],  mylongs is %d", rf.me, args.LeaderId,len(rf.log))
		// if rf.votedFor == -1 {
		// 	rf.votedFor = args.LeaderId
		// }

		// append log
		// logIdx := args.PrevLogIndex + 1
		// logChanged := false
		// for _, entry := range args.Entries {
		// 	if len(rf.log) > logIdx && rf.log[logIdx].Term != entry.Term {
		// 		rf.log = rf.log[:logIdx]
		// 		logChanged = true
		// 	}

		// 	rf.log = append(rf.log, entry)
		// 	logChanged = true
		// 	logIdx++
		// }

		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

		// if logChanged {
		rf.persist()
		// }

		// DPrintf1(2, "[%d] received RequestAppendEntries with %d logs, mylongs is %d", rf.me, len(args.Entries), len(rf.log))

		if args.LeaderCommit > rf.commitIndex {
			rf.setCommitIdx(min(args.LeaderCommit, len(rf.log)-1))
			// rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
	} else {
		reply.Sucess = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) processVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		// DPrintf1(4, "processVoteReply: [%d] changeToFollower!", rf.me)
		rf.changeToFollower(reply.Term, -1)
		return
		// stopTimer(rf.heartbeatTimer)
	}

	if reply.VotedGranted && rf.getRole() == Candidate && reply.Term == rf.currentTerm {
		// DPrintf1(2, "[%d] Vote Received!", rf.me)
		rf.voteReceived++
		// rf.voteReceived++
		if rf.voteReceived > len(rf.peers)/2 {
			// DPrintf("[%d] become leader of term[%d]", rf.me, rf.currentTerm)
			rf.changeToLeader()
			// stopTimer(rf.electionTimer)
			// DPrintf1(4, "[%d] broadcast appendentries starts", rf.me)
			go rf.startHeartbeat()
			// DPrintf1(4, "[%d] broadcast appendentries ends", rf.me)
			// resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
		}
	}
}

func (rf *Raft) processAppendEntries(reply *RequestAppendEntriesReply, peersIdx, logcnt, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.getRole() != Leader {
		return
	}

	if rf.currentTerm != term {
		return
	}

	if !reply.Sucess {
		if reply.Term > rf.currentTerm {
			// DPrintf1(4, "processAppendEntries: [%d] changeToFollower!", rf.me)
			rf.changeToFollower(reply.Term, -1)
		} else {
			rf.nextIndex[peersIdx]--
			go rf.sendAppendEntries(peersIdx, true)
		}
	} else {
		rf.nextIndex[peersIdx] += logcnt
		if rf.nextIndex[peersIdx] > len(rf.log) {
			rf.nextIndex[peersIdx] = len(rf.log)
			DPrintf1(5, "Node %d log overflow! next: %d, delta: %d, loglen: %d", peersIdx, rf.nextIndex[peersIdx], logcnt, len(rf.log))
		}
		rf.matchIndex[peersIdx] = rf.nextIndex[peersIdx] - 1

		// rf.nextIndex[peersIdx] = len(rf.log)
		// rf.matchIndex[peersIdx] = len(rf.log) - 1
		candiCommitIdx := len(rf.log) - 1
		// DPrintf1(3, "rf.nextIndex = %v", rf.nextIndex)
		// DPrintf1(3, "rf.matchIndex = %v", rf.matchIndex)
		// DPrintf1(3, "candiCommitIdx = %v, rf.commitIndex = %v", candiCommitIdx, rf.commitIndex)
	outerloop:
		for candiCommitIdx > rf.commitIndex {
			count := 0
			for i := 0; i < len(rf.matchIndex); i++ {
				// DPrintf1(3, "candiCommitIdx = %v, count = %v, i = %v, rf.matchIndex[i] = %v", candiCommitIdx, count, i, rf.matchIndex[i])
				if rf.matchIndex[i] >= candiCommitIdx {
					count++
					if count > len(rf.matchIndex)/2 {
						if rf.log[candiCommitIdx].Term == rf.currentTerm {
							rf.setCommitIdx(candiCommitIdx)
							break outerloop
						}
					}
				}
				// DPrintf1(3, "count = %v", count)
			}
			candiCommitIdx--
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
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
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.peers[server].Call("Raft.RequestVote", args, reply) {
				rf.processVoteReply(reply)
			}
		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int, withEntry bool) {
	rf.mu.Lock()
	if rf.getRole() != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIdx := rf.nextIndex[server] - 1
	// DPrintf1(5, "broadcastAppendEntries:server = [%d], prevLogIdx = [%d], loglen = [%d]", server, prevLogIdx, len(rf.log))
	args := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  rf.log[prevLogIdx].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]LogEntry, 0),
	}
	if withEntry {
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
	}

	// if len(args.Entries) > 0 {
	// 	DPrintf1(5, "replicating log", "to whom",id, "arg", arg,"nextIndex",raft.NextIndex[id])
	// }

	rf.mu.Unlock()

	reply := &RequestAppendEntriesReply{}
	DPrintf1(2, "[%d] Send RequestAppendEntriesArgs to [%d]: Term: %d, LeaderId: %d, LeaderCommit: %d", rf.me, server, args.Term, args.LeaderId, args.LeaderCommit)
	if rf.peers[server].Call("Raft.RequestAppendEntries", args, reply) {
		rf.processAppendEntries(reply, server, len(args.Entries), args.Term)
	}
}

func (rf *Raft) broadcastAppendEntries(withEntry bool) {
	// rf.mu.Lock()
	// rf.mu.Unlock()
	// rf.nextIndex[rf.me] = len(rf.log)
	// rf.matchIndex[rf.me] = len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntries(i, withEntry)
	}
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.changeToCandidate()
	rf.mu.Unlock()
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
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.isLeader
	if isLeader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		rf.persist()
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

func stopTimer(timer *time.Timer) {
	timer.Stop()
	select {
	case <-timer.C:
	default:
	}

	// rf.electionTimer.Reset(getElectTimeOut())
}

func resetTimer(timer *time.Timer, du time.Duration) {
	stopTimer(timer)
	timer.Reset(du)
}

func (rf *Raft) startElectionTimer() {
	// resetTimer(rf.electionTimer, getElectTimeOut())
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			role := rf.getRole()
			rf.mu.Unlock()
			if role == Leader {
				stopTimer(rf.electionTimer)
				// return
			} else {
				DPrintf1(4, "[%d] [%d] start new Election", rf.me, rf.getRole())
				rf.startNewElection()
				resetTimer(rf.electionTimer, getElectTimeOut())
			}

		case <-rf.killChan:
			return
		}
	}
}

func (rf *Raft) startHeartbeat() {
	DPrintf1(5, "%d start heartbeat", rf.me)
	defer DPrintf1(5, "%d end heartbeat", rf.me)
	rf.broadcastAppendEntries(true)
	resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())

	for {
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			role := rf.getRole()
			rf.mu.Unlock()

			if role == Leader {
				// stopTimer(rf.heartbeatTimer)
				rf.broadcastAppendEntries(true)
				resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
			} else {
				stopTimer(rf.heartbeatTimer)
				return
			}

		case <-rf.killChan:
			return
		}
	}
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
	rf.electionTimer = time.NewTimer(getElectTimeOut())
	rf.heartbeatTimer = time.NewTimer(getHeartBeatTimeOut())
	rf.killChan = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()
	// go rf.runHeartbeatTimer()

	return rf
}
