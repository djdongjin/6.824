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
	"time"
	"sync"
	"sync/atomic"
	"../labrpc"
	"math"
	"math/rand"
)

// import "bytes"
// import "../labgob"



// ApplyMsg : returned to services.
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
}

// Raft : A Go object implementing a single Raft peer.
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
	// persistent
	currentRole RaftRole		  // Leader, Candidate, Follower
	currentTerm int				  // latest term the serer has seen, init to 0
	voted		bool			  // if voted, then votedFor is this server's vote
	votedFor 	int 			  // candidateId the server voted for in current term, nil if none
	logs 		[]LogEntry		  // each LogEntry contains command, term when received by leader, first index = 1
	// volatile
	commitIndex int				  // highest log committed, init to 0 (none was committed, logs start at 1)
	lastApplied int				  // highest log applied to state machine, init to 0
	// volatile on leaders only
	// *re-init after each election*
	nextIndex 	[]int			  // next log entry to send to a server, init to leader's last log + 1
	matchIndex	[]int			  // highest log entry known to be replicated on that server, init to 0
	// helper variables
	lastTick	time.Time	 	  // election power, set true at the begining
								  // if still true after timeout, then electing, AppendEntries will set it to false
	voteCount	 int			  // record #vote granted during election
	voteFinished int			  // record #vote finished during election  
	voteFailed	 bool
}

//
// helper types
//

// RaftRole : one of three roles
type RaftRole int
const (
	leader	  RaftRole = 0
	candidate RaftRole = 1
	follower  RaftRole = 2
)

// LogEntry : a LogEntry contains command and term.
type LogEntry struct {
	Term int
	Index int
}

// GetState : return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentRole == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// helper functions for Raft
//

// find the slide idx in rf.logs that corresponds to the `index` log.
// return -1 if the log with index is not found.
// attention: call it inside rf.mu.Lock(), as the function itself doesn't check lock.
func (rf *Raft) findIdxOfIndex(index int) (int) {
	for l := len(rf.logs) - 1; l >= 0; l-- {
		if rf.logs[l].Index == index {
			return l
		}
	}
	return -1
}

// return the index and term of the last log,
// return (0, 0) if no log.
func (rf *Raft) lastLogIndexTerm() (int, int) {
	lenLogs := len(rf.logs)
	if lenLogs == 0 {
		return 0, 0
	} else {
		return rf.logs[lenLogs-1].Index, rf.logs[lenLogs-1].Term
	}
}

// check if the candidate's log, specified by params, is at least up-to-date as rf.
func (rf *Raft) behind(lastLogIndex, lastLogTerm int) bool {
	rfLastLogIndex, rfLastLogTerm := rf.lastLogIndexTerm()
	return lastLogTerm > rfLastLogTerm || lastLogTerm == rfLastLogTerm && lastLogIndex >= rfLastLogIndex
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		 int	// candidate's term
	CandidateId  int	// candidateId (rf.me)
	LastLogIndex int	// candidate's last log entry (correspond to rf.logs[-1])
	LastLogTerm  int	// term of last log entry (correspond to rf.logs[-1].term)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int		// follower.currentTerm, for candidate to update itself if needed
	VoteGranted bool	// true if received vote (remember to change follower.voteFor)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("%v <- RequestVote <- %v, curTerm: %v <- %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// candidate term is smaller, refuse.
		return
	} else if args.Term > rf.currentTerm {
		// next term, reset vote information first
		rf.currentTerm = args.Term
		rf.voted = false
		rf.currentRole = follower
		rf.votedFor = -1
	} 
	// whether vote for candidate, Figure 2. RequestVote Impl 2.
	if (!rf.voted || rf.votedFor == args.CandidateId) && rf.behind(args.LastLogIndex, args.LastLogTerm) {
		rf.voted = true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastTick = time.Now()
	}
	DPrintf("%v vote %v to %v\n", rf.me, reply.VoteGranted, args.CandidateId)
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
	DPrintf("%v -> sendRequestVote -> %v.\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) leaderElection() {
	DPrintf("%v <- leaderElection!", rf.me)
	t := time.Now()		// used for debugging
	for {
		DPrintf("%v electing, last time: %v\n", rf.me, time.Since(t))
		t = time.Now()
		rf.mu.Lock()
		killed := rf.killed()
		role := rf.currentRole
		rf.lastTick = time.Now()		// start timeout before election
		rf.mu.Unlock()
		if killed || role == leader {
			// server is closed or already elected, end leaderElection
			break
		}
		// 0. start timeout
		timeout := time.Duration(rand.Intn(150) + 300) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		timeElapse := time.Since(rf.lastTick)
		rf.mu.Unlock()
		if timeElapse < timeout {
			// heartbeat|AppendEntries RPC received, next round
			continue
		}
		// 1. do election
		rf.mu.Lock()
		rf.currentTerm++
		rf.currentRole = candidate
		rf.voted = true
		rf.votedFor = rf.me
		// prepare params for RequestVote RPC.
		lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
		rf.voteCount = 1
		rf.voteFinished = 1
		rf.voteFailed = false
		numPeers, me, curTerm := len(rf.peers), rf.me, rf.currentTerm	// to unlock earlier
		rf.mu.Unlock()
		cond := sync.NewCond(&rf.mu)
		// spread out RequestVote RPC
		for serverIdx := 0; serverIdx < numPeers; serverIdx++ {
			if serverIdx == me {
				continue
			}
			// sendRequestVote RPC closure
			go func(serverIdx, term, candidateId, lastLogIndex, lastLogTerm int) {
				args := RequestVoteArgs{Term:term, CandidateId:candidateId,
										LastLogIndex:lastLogIndex, LastLogTerm:lastLogTerm}
				reply := RequestVoteReply{}
				rf.sendRequestVote(serverIdx, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.voteFinished++
				if reply.VoteGranted {
					rf.voteCount++
				} else if reply.Term > rf.currentTerm {
					// someone has higher term, election failed, change to follower.
					rf.currentTerm = reply.Term
					rf.voteFailed = true
					rf.currentRole = follower
				}
				cond.Broadcast()
			} (serverIdx, curTerm, me, lastLogIndex, lastLogTerm)
		}
		// 3. election result
		rf.mu.Lock()
		for rf.voteCount < numPeers / 2 && rf.voteFinished < numPeers {
			cond.Wait()
		}
		DPrintf("%v election results: %v finised, %v voted, voteInvalid: %v\n", rf.me, rf.voteFinished, rf.voteCount, rf.voteFailed)
		if rf.voteFailed {
			// 3.1: other server claims leadership, convert to follower (already)
		} else if rf.voteCount > numPeers / 2 {
			// 3.2: majority vote, succeed.
			DPrintf("%v becomes leader!!\n", rf.me)
			rf.currentRole = leader
			for idx := 0; idx < numPeers; idx++ {
				rf.nextIndex[idx] = lastLogIndex + 1
				rf.matchIndex[idx] = 0
			}
			go rf.heartbeat()
			rf.mu.Unlock()
			break
		} else {
			// 3.3: non-majority vote, fail.
			// no extra actions, just restart election.
		}
		rf.mu.Unlock()
	}
	DPrintf("%v end leaderElection\n", rf.me)
}

//
// structs and RPCs for appendEntries
//

type AppendEntriesArgs struct {
	Term		 int		// leader's term
	LeaderId 	 int		// follower uses it to redirect clients
	PrevLogIndex int		// index of log entry immediately preceding new ones
	PrevLogTerm	 int		// term of PrevLogIndex logentry
	Entries		 []LogEntry	// to be stored in this server, empty for heartbeat
	LeaderCommit int		// leader's CommitIndex
}

type AppendEntriesReply struct {
	Term	int				// server's currentTerm, for leader to update itself
	Success bool			// true if follower contained entry matching prevLogIndex, prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v <- AppendEntries <- %v, term: (%v <- %v).", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// leader term < follower term
	if args.Term < rf.currentTerm {
		reply.Success = false;
		return
	}
	// clear conflict LogEntry
	idxOfIndex := rf.findIdxOfIndex(args.PrevLogIndex)
	if idxOfIndex == -1 || rf.logs[idxOfIndex].Index != args.PrevLogTerm {
		reply.Success = false;
		if idxOfIndex != -1 {
			rf.logs = rf.logs[:idxOfIndex]
		}
	}
	// append new LogEntries
	if len(args.Entries) > 0 {
		// TODO 2B. add new entries
		args.Entries = args.Entries[:]
	}
	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.logs[len(rf.logs)-1].Index)))
	}
	// close heartbeat power
	rf.lastTick = time.Now() 
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool) {
	DPrintf("%v -> sendAppendEntries -> %v\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartbeat() {
	DPrintf("%v <- heartbeat.\n", rf.me)
	for {
		time.Sleep(time.Duration(150) * time.Millisecond)
		rf.mu.Lock()
		role := rf.currentRole
		killed := rf.killed()
		rf.mu.Unlock()
		if role != leader || killed {
			break
		}
		rf.mu.Lock()
		prevLogIdx, prevLogTerm := rf.lastLogIndexTerm()
		for serverIdx := range rf.peers {
			if serverIdx == rf.me {
				continue
			}
			go func(serverIdx, term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []LogEntry) {
				args := AppendEntriesArgs{
					Term:term, LeaderId:leaderId,
					PrevLogIndex:prevLogIndex, PrevLogTerm:prevLogTerm,
					Entries:entries, LeaderCommit:leaderCommit}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(serverIdx, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !reply.Success && reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.currentRole = follower
				}
			} (serverIdx, rf.currentTerm, rf.me, prevLogIdx, prevLogTerm, rf.commitIndex, nil)
		}
		rf.mu.Unlock()
	}
	DPrintf("%v end heartbeat.\n", rf.me)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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
	DPrintf("%v is online now.\n", me)
	rf.currentRole = follower
	rf.currentTerm = 0
	rf.voted = false
	rf.votedFor = 0
	rf.logs = make([]LogEntry, 1)
	// volatile data
	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile data on leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastTick = time.Now()
	//
	go rf.leaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
