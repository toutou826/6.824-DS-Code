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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const ElectionTimeoutMinMs = 300
const ElectionTimeoutMaxMs = 450
const HeartbeatIntervalMs = 100
const electionTimeoutCheck = 10

const (
	Leader    = iota
	Follower  = iota
	Candidate = iota
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term         int
	Index        int
	CommandValid bool
	Command      interface{}
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIndcludedTerm int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm   int
	votedFor      int
	voteCount     int
	snapshotIndex int
	snapshotTerm  int
	log           []Log

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	logCond     *sync.Cond

	state       int
	leaderState LeaderState

	electionTimer time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	// DPrintf("GetState: After lock")

	defer rf.mu.Unlock()
	// defer DPrintf("GetState: After Unlock")

	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) getRaftStateByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.getRaftStateByte())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// rf.mu.Lock()
	// DPrintf("readPersist: After lock")
	// defer rf.mu.Unlock()
	// defer DPrintf("readPersist: After Unlock")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var snapshotIndex int
	var snapshotTerm int
	var log []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		DPrintf("Decode Error")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		if rf.snapshotIndex >= 0 {
			go func() {
				msg := ApplyMsg{}
				msg.SnapshotValid = true
				msg.SnapshotIndex = snapshotIndex
				msg.SnapshotTerm = snapshotTerm
				msg.Snapshot = rf.persister.ReadSnapshot()
				rf.applyCh <- msg
			}()
		}

	}
}

func generateElectionTime() time.Duration {
	return time.Duration(ElectionTimeoutMinMs+rand.Int31n(ElectionTimeoutMaxMs-ElectionTimeoutMinMs)) * time.Millisecond
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer = time.Now().Add(generateElectionTime())
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// DPrintf("InstallSnapshot: After lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("InstallSnapshot: After unlock")

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.resetElectionTimeout()

	if args.LastIncludedIndex <= rf.snapshotIndex || args.LastIncludedIndex < rf.lastApplied {
		return
	}

	// has lastIncluded log, retain log entries following it
	if args.LastIncludedIndex-rf.snapshotIndex < len(rf.log) &&
		rf.log[args.LastIncludedIndex-rf.snapshotIndex-1].Term == args.LastIndcludedTerm {
		rf.snapshotTerm = rf.log[args.LastIncludedIndex-rf.snapshotIndex-1].Term
		rf.log = rf.log[args.LastIncludedIndex-rf.snapshotIndex:]
	} else {
		// remove all log
		rf.log = []Log{}
	}

	// DPrintf(fmt.Sprint(snapshot))
	rf.snapshotIndex = args.LastIncludedIndex
	rf.saveStateAndSnapshot(args.Data)

	rf.commitIndex = max(rf.snapshotIndex, rf.commitIndex)
	rf.lastApplied = max(rf.snapshotIndex, rf.lastApplied)

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.SnapshotTerm = args.LastIndcludedTerm
	msg.Snapshot = args.Data

	if !rf.killed() {
		rf.applyCh <- msg
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	// DPrintf("AppendEntries: After lock")

	defer rf.mu.Unlock()
	defer rf.persist()
	// defer DPrintf("AppendEntries: After Unlock")

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	lastLogIndex := rf.getLastLogIndex()

	reply.ConflictIndex = lastLogIndex + 1
	reply.ConflictTerm = -1

	if args.Term > rf.currentTerm {
		if rf.state == Leader {
			rf.resetElectionTimeout()
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	if rf.state == Candidate {
		rf.state = Follower
	}

	rf.resetElectionTimeout()

	if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Index <= rf.snapshotIndex {
		return
	}

	if args.PrevLogIndex > rf.snapshotIndex && (args.PrevLogIndex > lastLogIndex || rf.log[args.PrevLogIndex-rf.snapshotIndex-1].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false

		// conflict log
		if args.PrevLogIndex <= lastLogIndex {

			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.snapshotIndex-1].Term

			firstConflict := args.PrevLogIndex
			// search first index for conflictTerm
			for i := args.PrevLogIndex; i > rf.commitIndex; i-- {
				if rf.log[i-rf.snapshotIndex-1].Term == rf.log[args.PrevLogIndex-rf.snapshotIndex-1].Term {
					firstConflict = i
				}
			}
			// remove conflict index and all that follow it
			rf.log = rf.log[:firstConflict-rf.snapshotIndex-1]
		}
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	iter := 0

	// DPrintf(fmt.Sprint(rf.me) + " received AE from " + fmt.Sprint(args.LeaderID) + " :" + fmt.Sprint(args.Entries))

	// if there is conflict, delete the conflict entry and all taht follow it
	for ; iter < len(args.Entries); iter++ {

		leaderLog := args.Entries[iter]

		// contains log that current server does not have
		if leaderLog.Index > lastLogIndex {
			break
		}

		if leaderLog.Index <= rf.snapshotIndex {
			continue
		}

		// unmatch, delete existing and all that follow
		if rf.log[leaderLog.Index-rf.snapshotIndex-1].Term != leaderLog.Term {
			rf.log = rf.log[:leaderLog.Index-rf.snapshotIndex-1]
			break
		}
	}

	// append new entries not already in the log
	if iter < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[iter:]...)
	}

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.logCond.Broadcast()
	}

}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	// DPrintf("RequestVote: After lock")

	defer rf.mu.Unlock()
	// defer DPrintf("RequestVote: After Unlock")

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	defer rf.persist()

	/* If RPC request or response contains term T > currentTerm:
	set currentTerm = T, convert to follower */
	if args.Term > rf.currentTerm {
		if rf.state == Leader {
			rf.resetElectionTimeout()
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.logIsMoreUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetElectionTimeout()
		return
	}
}

func (rf *Raft) logIsMoreUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {

	currentLstLogTerm := rf.getLastLogTerm()
	if currentLstLogTerm == candidateLastLogTerm {
		return candidateLastLogIndex >= rf.getLastLogIndex()
	}
	return candidateLastLogTerm > currentLstLogTerm
}

func (rf *Raft) requestVoteFromPeers(currentTerm int, lastLogTerm int, lastLogIndex int) {

	args := RequestVoteArgs{}
	args.CandidateID = rf.me
	args.Term = currentTerm
	args.LastLogTerm = lastLogTerm
	args.LastLogIndex = lastLogIndex

	for i := 0; i < len(rf.peers); i++ {

		// skip itself
		if i == rf.me {
			continue
		}

		// request votes in parallel
		go func(index int) {

			rf.callRequestVote(index, &args)

		}(i)
	}
}

func (rf *Raft) callAppendEntries(peer int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, &args, &reply)

	rf.mu.Lock()
	// DPrintf("callAppendEntries: After lock")

	defer rf.mu.Unlock()
	// defer DPrintf("callAppendEntries: After Unlock")

	// outdated response
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	// revert to follower if term is greater
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimeout()
		return
	}

	// update leaderState
	if ok {

		if reply.Success {
			// update state
			rf.leaderState.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.leaderState.nextIndex[peer] = rf.leaderState.matchIndex[peer] + 1

			// update commitIndex
			for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-rf.snapshotIndex-1].Term == rf.currentTerm; N-- {

				count := 1

				// count how many matchIndex[i] >= N
				for i, val := range rf.leaderState.matchIndex {
					if i != rf.me && val >= N {
						count++
					}
				}

				// satisfy the constrain, update commitIndex to N
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					rf.logCond.Broadcast()
					break
				}
			}
		} else {
			// AppendEntries failed, decrement nextIndex
			if reply.ConflictTerm != -1 {
				for i := args.PrevLogIndex; i > rf.commitIndex; i-- {
					if rf.log[i-rf.snapshotIndex-1].Term == reply.ConflictTerm {
						rf.leaderState.nextIndex[peer] = min(i+1, rf.getLastLogIndex())
						return
					}
				}
			}
			rf.leaderState.nextIndex[peer] = min(reply.ConflictIndex, rf.getLastLogIndex())
		}
	}
}

func (rf *Raft) callRequestVote(peer int, args *RequestVoteArgs) {

	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(peer, args, &reply)

	// candidate update term

	rf.mu.Lock()
	// DPrintf("callRequestVote: After lock")
	defer rf.mu.Unlock()
	// defer DPrintf("callRequestVote: After Unlock")

	if rf.state != Candidate || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		if rf.state == Leader {
			rf.resetElectionTimeout()
		}
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if ok && reply.VoteGranted {
		rf.voteCount++

		if rf.voteCount > len(rf.peers)/2 {
			// become leader
			rf.state = Leader

			// dummy
			// rf.log = append(rf.log, Log{rf.currentTerm, rf.getLastLogIndex() + 1, nil})
			rf.persist()

			// initialize leader state
			rf.leaderState = LeaderState{}
			rf.leaderState.matchIndex = []int{}
			rf.leaderState.nextIndex = []int{}
			index := rf.getLastLogIndex()

			for i := range rf.peers {
				if i == rf.me {
					rf.leaderState.nextIndex = append(rf.leaderState.nextIndex, -1)
					rf.leaderState.matchIndex = append(rf.leaderState.matchIndex, -1)
				} else {
					rf.leaderState.nextIndex = append(rf.leaderState.nextIndex, index+1)
					rf.leaderState.matchIndex = append(rf.leaderState.matchIndex, -1)
				}
			}

			go rf.sendAppendEntriesRPCLoop()

		}
	}
}

func (rf *Raft) callInstallSnapshot(peer int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, &args, &reply)

	rf.mu.Lock()
	// DPrintf("callInstallSnapshot: After lock")
	defer rf.mu.Unlock()
	// defer DPrintf("callInstallSnapshot: After Unlock")

	// outdated response
	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	// revert to follower if term is greater
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimeout()
		return
	}

	// udpdate leader state
	rf.leaderState.matchIndex[peer] = args.LastIncludedIndex
	rf.leaderState.nextIndex[peer] = rf.leaderState.matchIndex[peer] + 1
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Start: After lock")

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// append to log if server is a leader
	if isLeader {
		rf.log = append(rf.log, Log{term, index, true, command})
		// DPrintf("Added new Log: " + fmt.Sprint(command) + " to leader " + fmt.Sprint(rf.me))
		rf.persist()
		go rf.startAppendEntries()
	}

	// DPrintf("Start: After Unlock")

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.snapshotIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// After election timeout, the server starts an election
func (rf *Raft) attemptElection() {

	rf.mu.Lock()
	// DPrintf("attemptElection: After lock")
	rf.state = Candidate
	rf.currentTerm++    // increase term
	rf.votedFor = rf.me // votes for itself
	rf.voteCount = 1
	rf.persist()
	term := rf.currentTerm
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	rf.resetElectionTimeout()
	rf.mu.Unlock()
	// DPrintf("attemptElection: After Unlock")

	rf.requestVoteFromPeers(term, lastLogTerm, lastLogIndex)
}

func (rf *Raft) sendAppendEntriesRPCLoop() {

	for {

		// break if the program has already ended
		if rf.killed() {
			break
		}

		// break if no longer a leader
		rf.mu.Lock()
		// DPrintf("sendAppendEntriesRPCLoopCheckLeader: After lock")

		if rf.state != Leader {
			rf.mu.Unlock()
			// DPrintf("sendAppendEntriesRPCLoopCheckLeader: After Unlock")
			break
		}
		rf.mu.Unlock()
		// DPrintf("sendAppendEntriesRPCLoopCheckLeader: After Unlock")

		rf.startAppendEntries()
		// DPrintf("sendAppendEntriesRPCLoop: After Unlock")

		time.Sleep(time.Duration(HeartbeatIntervalMs) * time.Millisecond)
	}
}

func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	// DPrintf("sendAppendEntriesRPCLoop: After lock")
	currentSnapshot := rf.persister.ReadSnapshot()
	for i := range rf.peers {

		if rf.state != Leader {
			break
		}

		if i == rf.me {
			continue
		}
		// log entries starting at nextIndex

		// send AppendEntries
		if rf.leaderState.nextIndex[i] > rf.snapshotIndex {
			args := AppendEntriesArgs{}
			args.LeaderID = rf.me
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.leaderState.nextIndex[i] - 1
			if args.PrevLogIndex > rf.snapshotIndex {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.snapshotIndex-1].Term
			} else {
				args.PrevLogTerm = rf.snapshotTerm
			}
			if rf.getLastLogIndex() >= rf.leaderState.nextIndex[i] {
				// use deep copy to avoid data race
				args.Entries = make([]Log, len(rf.log[rf.leaderState.nextIndex[i]-rf.snapshotIndex-1:]))
				copy(args.Entries, rf.log[rf.leaderState.nextIndex[i]-rf.snapshotIndex-1:])
			}
			// DPrintf(fmt.Sprint(rf.me) + ": Sending to " + fmt.Sprint(i) + " log: " + fmt.Sprint(args.Entries))
			go rf.callAppendEntries(i, args)
		} else {
			// send InstallSnapshot
			args := InstallSnapshotArgs{}
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.LastIncludedIndex = rf.snapshotIndex
			args.LastIndcludedTerm = rf.snapshotTerm
			args.Data = currentSnapshot
			go rf.callInstallSnapshot(i, args)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) checkElectionTimeout() {
	for {
		time.Sleep(electionTimeoutCheck * time.Millisecond)

		// break if the program has already ended
		if rf.killed() {
			rf.mu.Lock()
			close(rf.applyCh)
			rf.mu.Unlock()
			break
		}

		rf.mu.Lock()
		state := rf.state
		now := time.Now()
		lastHeartBeat := rf.electionTimer
		rf.mu.Unlock()

		if state != Leader && now.After(lastHeartBeat) {
			go rf.attemptElection()
		}
	}
}

func (rf *Raft) applyCommits() {
	for {

		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.logCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			break
		}

		// DPrintf("ApplyCommit: After lock")
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{}
			msg.CommandValid = rf.log[i-rf.snapshotIndex-1].CommandValid
			msg.CommandIndex = i
			msg.Command = rf.log[i-rf.snapshotIndex-1].Command
			// DPrintf(fmt.Sprint(rf.me) + ": Sending command at index " + fmt.Sprint(msg.CommandIndex) + " commmand: " + fmt.Sprint(msg.Command))
			rf.applyCh <- msg
			rf.lastApplied = i
		}
		rf.mu.Unlock()
		// DPrintf("ApplyCommit: After Unlock")
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.snapshotIndex = -1
	rf.snapshotTerm = 0

	rf.log = []Log{}
	rf.log = append(rf.log, Log{0, 0, false, nil})

	rf.state = Follower
	rf.applyCh = applyCh
	rf.logCond = sync.NewCond(&rf.mu)

	rf.commitIndex = -1
	rf.lastApplied = -1

	go func() {
		// initialize from state persisted before a crash
		rf.readPersist(persister.ReadRaftState())
		go rf.checkElectionTimeout()
		go rf.applyCommits()
	}()

	return rf
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// oudated
		if index <= rf.snapshotIndex || index > rf.getLastLogIndex() {
			return
		}
		snapshotTerm := 0
		if index-rf.snapshotIndex-1 >= len(rf.log) {
			rf.log = []Log{}
		} else {
			// remove everything up to and including index
			snapshotTerm = rf.log[index-rf.snapshotIndex-1].Term
			rf.log = rf.log[index-rf.snapshotIndex:]
		}
		// DPrintf(fmt.Sprint(snapshot))
		rf.snapshotIndex = index
		rf.snapshotTerm = snapshotTerm
		rf.saveStateAndSnapshot(snapshot)
	}()
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	data := rf.getRaftStateByte()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
