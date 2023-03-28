package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive Logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	Term    int
	Index   int
	Command interface{}
}

// AppendEntriesArgs
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so followers can redirect clients
	PrevLogIndex int   // index of Logs entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // Logs entries to store
	LeaderCommit int   // leader's commitIndex
}

// AppendEntriesReply
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for candidate to update itself
	Success bool // true if follower contained entry matching  prevLogIndex and prevLogTerm

	// fields for tracking conflicts
	ConflictTerm   int
	ConflictIndex  int
	ConflictLength int
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last Logs
	LastLogTerm  int // term of candidate’s last Logs entry
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	CurrentTerm int // latest term server has seen
	VotedFor    int // candidateId that received vote in current term
	Logs        []Log

	// Volatile state on all servers:
	commitIndex int // index of the highest Logs entry known to be committed
	lastApplied int // index of the highest Logs entry applied to state machine

	// Volatile state on leaders:
	nextIndex  []int // index of the next Logs entry to send to that server
	matchIndex []int // index of the highest Logs entry known to be replicated on server

	// Additional fields:
	role          Role          // member can be a follower, leader or candidate
	lastHeartbeat time.Time     // time of the last heartbeat
	receivedVotes int           // received voted during term
	applyCh       chan ApplyMsg // communication channel for raft to send ApplyMsgs
	commitCh      chan bool     // communication channel, wakes up the applier goroutine
}

// Make
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
	rf.dead = 0

	rf.CurrentTerm = 0
	rf.VotedFor = NoVote
	rf.role = FOLLOWER
	rf.lastHeartbeat = time.Now()
	rf.receivedVotes = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	// since followers don't keep states of peers
	// and every peer starts off as a follower,
	// these should be nil or empty
	rf.matchIndex = nil
	rf.nextIndex = nil

	rf.Logs = make([]Log, 0)
	rf.Logs = append(rf.Logs, Log{Term: 0, Index: 0})

	rf.commitCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to apply messages
	go rf.applier()

	return rf
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
	}
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	isLeader := rf.role == LEADER
	if !isLeader {
		rf.mu.Unlock()
		return 0, 0, false
	}

	term := rf.CurrentTerm
	index := rf.nextIndex[rf.me]
	rf.Logs = append(rf.Logs, Log{
		Term:    term,
		Index:   index,
		Command: command,
	})

	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index
	rf.persist()

	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.appendEntriesSender(i, term)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) convertToFollower(term int, changeVote bool, votedFor int) {
	rf.CurrentTerm = term
	rf.receivedVotes = 0
	rf.role = FOLLOWER
	rf.lastHeartbeat = time.Now()

	rf.nextIndex = nil
	rf.matchIndex = nil

	if changeVote {
		rf.VotedFor = votedFor
	}
	rf.persist()
}

func (rf *Raft) requestFromPeer(peer int,
	args *RequestVoteArgs, ch chan<- bool) {
	vote := false
	defer func() { ch <- vote }()

	rf.mu.Lock()
	if (rf.role != CANDIDATE) || (rf.CurrentTerm != args.Term) || (rf.killed()) {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// redo the checks since the state of the server might have
	// changed before rpc was performed
	if rf.role != CANDIDATE || rf.killed() || !ok {
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.convertToFollower(reply.Term, true, NoVote)
		return
	}
	if rf.CurrentTerm != args.Term {
		return
	}
	vote = reply.VoteGranted
}

func (rf *Raft) convertToLeader() {
	rf.role = LEADER
	lastLog := rf.getLastLog()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// newly elected leader has no information about peers state,
	// so it initializes next indices to his next index
	// and match indices to 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastLog.Index
}

func (rf *Raft) countVotes(term int, ch <-chan bool) {
	cnt := 1
	victory := false
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-ch {
			cnt++
		}
		if !victory && cnt >= len(rf.peers)/2+1 {
			victory = true
			rf.mu.Lock()
			// redo checks
			if rf.role != CANDIDATE || rf.CurrentTerm != term {
				rf.mu.Unlock()
			} else {
				rf.convertToLeader()
				rf.mu.Unlock()
				go rf.sendHeartbeats(rf.CurrentTerm)
			}
		}
	}
}

func getRandomSleepTime() time.Duration {
	return time.Duration(NoHeartbeatsAlarmTime+rand.Intn(NoHeartbeatsRandomRange)) *
		time.Millisecond
}

// assumes raft is locked
func (rf *Raft) startElection() {
	rf.role = CANDIDATE
	rf.receivedVotes = 1
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.persist()

	lastLog := rf.getLastLog()

	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.CurrentTerm,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
	}
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
	electionCh := make(chan bool)
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestFromPeer(i, &args, electionCh)
		}
	}
	go rf.countVotes(args.Term, electionCh)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		if role == LEADER {
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
		} else {
			if rf.timeToStartElection() {
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		}
		time.Sleep(getRandomSleepTime())
	}
}

func (rf *Raft) timeToStartElection() bool {
	timeSinceLastHeartbeat := time.Since(rf.lastHeartbeat)
	electionCallingTime := time.Millisecond *
		time.Duration(NoHeartbeatsAlarmTime+NoHeartbeatsRandomRange)
	return timeSinceLastHeartbeat > electionCallingTime
}

func (rf *Raft) getLastLog() *Log {
	l := len(rf.Logs)
	if l > 0 {
		return &rf.Logs[l-1]
	}
	return &Log{}
}

// GetState
// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isLeader = rf.role == LEADER
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Logs) != nil {
	} else {
		data := w.Bytes()
		rf.persister.SaveRaftState(data)
	}
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Logs through (and including)
// that index. Raft should now trim its Logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	// Reply false if term < CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term, true, NoVote)
	}

	// If VotedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.VotedFor == NoVote || rf.VotedFor == args.CandidateId) &&
		rf.candidateLogIsNotBehind(args.LastLogIndex, args.LastLogTerm) {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	}
}

// AppendEntries
// example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = false

	// Reply false if term < CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	// at this point it's safe to reset the heartbeat time,
	// it might get reset again in this method,
	// but if so it'll happen soon so no biggie
	rf.lastHeartbeat = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm, find conflicting term, index and length
	lastLog := rf.getLastLog()
	if len(rf.Logs) <= args.PrevLogIndex {
		reply.ConflictLength = lastLog.Index + 1
		return
	}
	conflictingTerm := rf.Logs[args.PrevLogIndex].Term
	if conflictingTerm != args.PrevLogTerm {
		reply.ConflictTerm = conflictingTerm
		for i := args.PrevLogIndex; i > -1; i-- {
			reply.ConflictIndex = rf.Logs[i].Index
			if rf.Logs[i].Term != conflictingTerm {
				break
			}
		}
		return
	}
	reply.Success = true

	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term, true, NoVote)
	}

	// if candidate receives a request with term >= CurrentTerm (already a given),
	// it should assume that election is already lost and become a follower.
	if rf.role == CANDIDATE && args.Term >= rf.CurrentTerm {
		rf.convertToFollower(args.Term, true, NoVote)
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if len(args.Entries) > 0 {
		badEntries := rf.Logs[args.PrevLogIndex+1:]
		persist := false
		var appendFrom int
		for i := 0; i < min(len(badEntries), len(args.Entries)); i++ {
			if badEntries[i].Term != args.Entries[i].Term {
				rf.Logs = rf.Logs[:args.PrevLogIndex+i+1]
				persist = true
				appendFrom = i
				break
			}
		}

		if appendFrom < len(args.Entries) {
			rf.Logs = append(rf.Logs, args.Entries[appendFrom:]...)
			persist = true
		}

		if persist {
			rf.persist()
		}

	}

	// If leaderCommit > commitIndex, set
	// commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		go func() { rf.commitCh <- true }()
	}

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		// redo checks
		if rf.role != LEADER || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go rf.appendEntriesSender(i, rf.CurrentTerm)
			}
		}

		time.Sleep(HeartbeatSendingRate * time.Millisecond)
	}
}

// the appendEntriesSender goroutine sends args to peer and
// processes the response
func (rf *Raft) appendEntriesSender(peer, term int) {
	rf.mu.Lock()
	// redo checks
	if rf.role != LEADER || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}
	args := rf.appendEntriesArgsForPeer(peer, term)
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.convertToFollower(reply.Term, true, NoVote)
	}

	if rf.role != LEADER || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		// update nextIndex and matchIndex for follower
		rf.nextIndex[peer] = max(args.PrevLogIndex+len(args.Entries)+1,
			rf.nextIndex[peer])
		rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries),
			rf.matchIndex[peer])
		go rf.committer(term)
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry
		if reply.ConflictLength != 0 && reply.ConflictTerm == 0 {
			rf.nextIndex[peer] = reply.ConflictLength
		} else {
			for i := len(rf.Logs) - 1; i > -1; i-- {
				if rf.Logs[i].Term == reply.ConflictTerm {
					rf.nextIndex[peer] = rf.Logs[i].Index + 1
					break
				}
				if rf.Logs[i].Term < reply.ConflictTerm {
					rf.nextIndex[peer] = reply.ConflictIndex
					break
				}
			}
		}
		go rf.appendEntriesSender(peer, term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) appendEntriesArgsForPeer(peer, term int) *AppendEntriesArgs {
	lastLog := rf.getLastLog()
	lastLogPeer := rf.Logs[rf.nextIndex[peer]-1]

	var entries []Log
	if lastLog.Index >= rf.nextIndex[peer] {
		entries = rf.Logs[rf.nextIndex[peer]:]
	}

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: lastLogPeer.Index,
		PrevLogTerm:  lastLogPeer.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	return args
}

func (rf *Raft) candidateLogIsNotBehind(index, term int) bool {
	lastLog := rf.getLastLog()
	if lastLog.Term > term {
		return false
	}
	if lastLog.Term < term {
		return true
	}
	return index >= lastLog.Index
}

// The committer go routine checks if any Logs can be committed,
// writes to commitCh if so
func (rf *Raft) committer(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// redo checks
	if rf.role != LEADER || rf.CurrentTerm != term {
		return
	}

	lastLog := rf.getLastLog()
	if rf.commitIndex >= lastLog.Index {
		return
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == CurrentTerm:
	// set commitIndex = N
	originalCommitIndex := rf.commitIndex
	for i := rf.commitIndex + 1; i < len(rf.Logs); i++ {
		if rf.Logs[i].Term == term {
			committed := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					committed++
				}
			}
			if committed >= len(rf.peers)/2+1 {
				rf.commitIndex = i
			}
		}
	}

	if originalCommitIndex != rf.commitIndex {
		go func() { rf.commitCh <- true }()
	}
}

// the applier goroutine is started at creation and wakes up anytime
// something is written inside the commitCh.
// It goes through all the non-applied committed Logs and applies them
func (rf *Raft) applier() {
	for !rf.killed() {
		<-rf.commitCh

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			log := rf.Logs[rf.lastApplied]

			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}

			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}
