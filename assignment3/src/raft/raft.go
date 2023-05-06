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
	"math/rand"
	"src/labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const(
	ELECT_TIMEOUT_MIN = 150
	ELECT_TIMEOUT_MAX = 300
	HEATBEAT_TIMEOUT = 100
)

func getElectTimeOut() time.Duration {
	interval := ELECT_TIMEOUT_MIN + rand.Intn(ELECT_TIMEOUT_MAX - ELECT_TIMEOUT_MIN)
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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	killChan    chan struct{}

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	isLeader    bool
    voteReceived int
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) getRole() (role RaftRole) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()	
	if rf.isLeader {
		role = Leader
	} else if rf.votedFor == rf.me {
		role = Candidate
	}else{
		role = Follower
	}

	return
}

func (rf *Raft) changeToFollower (term int, votedFor int) {
	rf.isLeader = false
	rf.currentTerm = term
	rf.votedFor = votedFor
}

func (rf *Raft) changeToCandidate () {
	rf.isLeader = false
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteReceived = 1
}

func (rf *Raft) changeToLeader () {
	rf.isLeader = true
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term            int
	VotedGranted    bool
}

type RequestAppendEntriesArgs struct {
	// Your data here.
	Term            int
	LeaderId        int
}

type RequestAppendEntriesReply struct {
	// Your data here.
	Term            int
	Sucess          bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}

	if args.Term == rf.currentTerm && rf.votedFor == -1 {
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

func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}

	if args.Term == rf.currentTerm && (args.LeaderId == rf.votedFor || rf.votedFor == -1) {
		reply.Sucess = true
		reply.Term = args.Term
		DPrintf("[%d] received RequestAppendEntries from [%d]", rf.me, args.LeaderId)
		if rf.votedFor == -1 {
			rf.changeToFollower(args.Term, args.LeaderId)
		}
		resetTimer(rf.electionTimer, getElectTimeOut())
		DPrintf("[%d] timer reseted", rf.me)
	}else{
		reply.Sucess = false
		reply.Term = rf.currentTerm		
	}
}

func (rf *Raft) processVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term, -1)
	}

	if reply.VotedGranted && rf.getRole() == Candidate{
		rf.voteReceived++
		if rf.voteReceived > len(rf.peers) / 2 {
			DPrintf("[%d] become leader of term[%d]", rf.me, rf.currentTerm)
			rf.changeToLeader()
			stopTimer(rf.electionTimer)
			rf.broadcastAppendEntries()
			resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
		}
	}
}

func (rf *Raft) processAppendEntries(reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if rf.GetRole() != Leader {
	// 	return
	// }
	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term, -1)
	}

}


func (rf *Raft) broadcastRequestVote() {
	// rf.mu.Lock()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	// rf.mu.Unlock()
    for i:= 0; i < len(rf.peers); i++ {
		if i == rf.me {continue}
		go func (server int)  {
			reply := &RequestVoteReply{}
			if rf.peers[server].Call("Raft.RequestVote", args, reply) {
				rf.processVoteReply(reply)
			}
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	// rf.mu.Lock()
	args := RequestAppendEntriesArgs{Term: rf.currentTerm, LeaderId : rf.me}
	// rf.mu.Unlock()
    for i:= 0; i < len(rf.peers); i++ {
		if i == rf.me {continue}
		go func (server int)  {
			reply := &RequestAppendEntriesReply{}
			if rf.peers[server].Call("Raft.RequestAppendEntries", args, reply) {
				rf.processAppendEntries(reply)
			}
		}(i)
	}
}

func (rf *Raft) startNewElection() {
	rf.changeToCandidate()
	rf.broadcastRequestVote()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
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

func (rf *Raft) runElectionTimer(){
	for {
		select {
		case <- rf.electionTimer.C:
			rf.mu.Lock()
			if rf.getRole() == Leader {
				stopTimer(rf.electionTimer)
			}else{
				DPrintf("[%d] [%d] start new Election", rf.me, rf.getRole())
				rf.startNewElection()
				resetTimer(rf.electionTimer, getElectTimeOut())				
			}
			
			rf.mu.Unlock()
		case <-rf.killChan:
			return
		}
	}
}

func (rf *Raft) runHeartbeatTimer(){
	for {
		select {
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			
			if rf.getRole() == Leader {
				rf.broadcastAppendEntries()
				resetTimer(rf.heartbeatTimer, getHeartBeatTimeOut())
			} else {
				stopTimer(rf.heartbeatTimer)
			}
			
			rf.mu.Unlock()
		case <-rf.killChan:
			return
		}	
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(getElectTimeOut())
	rf.heartbeatTimer = time.NewTimer(getHeartBeatTimeOut())
	rf.killChan = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()
	go rf.runHeartbeatTimer()
		
	return rf
}
