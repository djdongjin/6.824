package raft

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

import (
	"time"
	"sync"
	"sync/atomic"
	"../labrpc"
	"../labgob"
	"math"
	"math/rand"
	"fmt"
	"bytes"
)


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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// Raft : A Go object implementing a single Raft peer.
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
	votedFor 	int 			  // candidateId the server voted for in current term, nil if none
	logs 		[]LogEntry		  // each LogEntry contains command, term when received by leader, first index = 1
	numCompact	int				  // save #(compacted logs)
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
	applyCh		chan ApplyMsg
}

// helper types

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
	Command interface{}
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.currentRole)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.numCompact)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var a RaftRole
	var term int
	var vote int
	var logs []LogEntry
	var compact int
	if d.Decode(&term) != nil ||
	   d.Decode(&vote) != nil ||
	   d.Decode(&logs) != nil ||
	   d.Decode(&compact) != nil {
        DPrintf("readPersist error!!!")
	} else {
		// rf.currentRole = a
		rf.currentTerm = term
		rf.votedFor    = vote
		rf.logs 	   = logs
		rf.numCompact  = compact
	}
}


// helper functions for Raft

// find the slide idx in rf.logs that corresponds to the `index` log.
// return -1 if the log with index is not found.
// attention: call it inside rf.mu.Lock(), as the function itself doesn't check lock.
func (rf *Raft) findIdxOfIndex(index int) (int) {
	if index >= len(rf.logs) {
		return -1
	}
	return index
}

// return the index and term of the last log,
//  (0, 0) is the default log at the begining.
func (rf *Raft) lastLogIndexTerm() (int, int) {
	lenLogs := len(rf.logs)
	return lenLogs - 1, rf.logs[lenLogs - 1].Term
}

// check if the candidate's log, specified by params, is at least up-to-date as rf.
func (rf *Raft) behind(lastLogIndex, lastLogTerm int) bool {
	rfLastLogIndex, rfLastLogTerm := rf.lastLogIndexTerm()
	return lastLogTerm > rfLastLogTerm || lastLogTerm == rfLastLogTerm && lastLogIndex >= rfLastLogIndex
}

func (rf *Raft) checkRoleTerm(role RaftRole, term int) bool {
	return role == rf.currentRole && term == rf.currentTerm
}

func (rf *Raft) changeToLeader(term int) {
	DPrintf("changeToLeader(%v) Term(%v->%v)\n", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.currentRole = leader
	nextLogIndex := len(rf.logs)
	DPrintf("New Leader (%v) change nextIndex to (%v), len(log)(%v)\n", rf.me, nextLogIndex, len(rf.logs))
	for idxPeer := range rf.peers {
		rf.nextIndex[idxPeer] = nextLogIndex
		rf.matchIndex[idxPeer] = 0
	}
}

func (rf *Raft) changeToCandidate(term int) {
	DPrintf("changeToCandidate(%v) Term(%v->%v)\n", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.currentRole = candidate
	rf.votedFor = rf.me
}

func (rf *Raft) changeToFollower(term int) {
	DPrintf("changeToFollower(%v) Term(%v->%v)\n", rf.me, rf.currentTerm, term)
	if rf.currentRole == leader {
		go rf.leaderElection()
	}
	rf.currentTerm = term
	rf.currentRole = follower
	rf.votedFor = -1
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		 int	// candidate's term
	CandidateId  int	// candidateId (rf.me)
	LastLogIndex int	// candidate's last log entry (correspond to rf.logs[-1])
	LastLogTerm  int	// term of last log entry (correspond to rf.logs[-1].term)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int		// follower.currentTerm, for candidate to update itself if needed
	VoteGranted bool	// true if received vote (remember to change follower.voteFor)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RequestVote (%v<-%v), Term: (%v<-%v)\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// 1. candidate term is smaller, refuse.
		DPrintf("RequestVote refused (%v<-%v), Term: (%v<-%v)\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	} else if args.Term > rf.currentTerm {
		// next term, reset vote information first
		rf.changeToFollower(args.Term)
		rf.persist()
	}
	// 2. whether vote for candidate, Figure 2. RequestVote Impl 2.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.behind(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.lastTick = time.Now()
	}
	DPrintf("RequestVote %v (%v<-%v), Term: (%v<-%v)\n", reply.VoteGranted, rf.me, args.CandidateId, rf.currentTerm, args.Term)
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("sendRequestVote (%v->%v), Term: (%v)\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) leaderElection() {
	DPrintf("leaderElection (%v) started \n", rf.me)
	t := time.Now()		// used for debugging
	for {
		DPrintf("leaderElection (%v), since last time: (%v)\n", rf.me, time.Since(t))
		t = time.Now()
		// 0. start timeout
		timeout := time.Duration(rand.Intn(150) + 300) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		timeElapse := time.Since(rf.lastTick)
		if rf.killed() || rf.currentRole == leader {
			// server is closed or already elected, end leaderElection
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		if timeElapse < timeout {
			// heartbeat|AppendEntries RPC received, next round
			DPrintf("leaderElection (%v) reset timer\n", rf.me)
		} else {
			// issue startVoting
			go rf.startVoting()
		}
	}
	DPrintf("leaderElection (%v) end\n", rf.me)
}

func (rf *Raft) startVoting() {
	// 1. do election
	rf.mu.Lock()
	rf.lastTick = time.Now()
	rf.changeToCandidate(rf.currentTerm + 1)
	rf.persist()
	// prepare params for RequestVote RPC.
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	numPeers, me, curTerm := len(rf.peers), rf.me, rf.currentTerm	// to unlock earlier
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)
	// spread out RequestVote RPC
	voteFinished, voteCount := 1, 1
	for idxPeer := 0; idxPeer < numPeers; idxPeer++ {
		if idxPeer == me {
			continue
		}
		// sendRequestVote RPC closure
		go func(idxPeer, term, candidateId, lastLogIndex, lastLogTerm int) {
			args := RequestVoteArgs{Term:term, CandidateId:candidateId,
									LastLogIndex:lastLogIndex, LastLogTerm:lastLogTerm}
			reply := RequestVoteReply{}
			for {
				ok := rf.sendRequestVote(idxPeer, &args, &reply)
				// only process reply if it isn't outdated
				rf.mu.Lock()
				if rf.killed() || !rf.checkRoleTerm(candidate, args.Term) {
					break
				}
				rf.mu.Unlock()
				// resend or process RPC request
				if !ok {
					time.Sleep(50 * time.Millisecond)
				} else {
					rf.mu.Lock()
					voteFinished++
					if reply.VoteGranted {
						voteCount++
					} else if reply.Term > rf.currentTerm {
						// someone has higher term, election failed, change to follower.
						rf.changeToFollower(reply.Term)
						rf.persist()
					}
					break
				}
			}
			cond.Broadcast()
			rf.mu.Unlock()
		} (idxPeer, curTerm, me, lastLogIndex, lastLogTerm)
	}
	// 3. election result
	rf.mu.Lock()
	for rf.currentRole == candidate && voteCount <= numPeers / 2 && voteFinished < numPeers {
		cond.Wait()
	}
	DPrintf("leaderElection (%v) results: #peers (%v), #finised (%v), #voted (%v)\n", 
		rf.me, len(rf.peers), voteFinished, voteCount)
    if rf.killed() || !rf.checkRoleTerm(candidate, curTerm) {
		// 3.1: other server claims leadership, convert to follower (already)
	} else if voteCount > numPeers / 2 {
		// 3.2: majority vote, succeed
		DPrintf("%v becomes leader!!\n", rf.me)
		rf.changeToLeader(rf.currentTerm)
		rf.persist()
		go rf.heartbeat()
	}
		// 3.3: non-majority vote, fail.
		// no extra actions, just restart election.
	rf.mu.Unlock()
}

// structs and RPCs for appendEntries

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

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntriesArgs: Term(%v) Leader(ID,Commit)(%v,%v) PrevLog(Idx,Term)(%v,%v) Entries(%v)", 
		args.Term, args.LeaderId, args.LeaderCommit,
		args.PrevLogIndex, args.PrevLogTerm, args.Entries)
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply: ServerTerm(%v) Success(%v)",
		reply.Term, reply.Success)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AppendEntries(%v<-%v) Term(%v<-%v) args: %s\n", 
		rf.me, args.LeaderId, rf.currentTerm, args.Term, args)
	reply.Term = rf.currentTerm
	reply.Success = false
	// 1. return false, since leader term < follower term
	if args.Term < rf.currentTerm {
		return
	}
	// now we can ensure that the RPC is from the current leader, so reset timer
	rf.lastTick = time.Now()
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
		rf.persist()
	}
	// 2. return false, since no log at PrevLogIndex match with leader
	idxOfPrevLog := rf.findIdxOfIndex(args.PrevLogIndex)
	if idxOfPrevLog == -1 || rf.logs[idxOfPrevLog].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintf("AppendEntries(%v<-%v) refused Term(%v<-%v) curLog(%v) args: %s\n", 
				rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.logs, args)
		return
	}
	// 3 & 4: clean conflict logs and append new logs
	reply.Success = true
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			idxOfNewLog := rf.findIdxOfIndex(entry.Index)
			if idxOfNewLog == -1 {
				// append new entries
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			} else if entry.Term != rf.logs[idxOfNewLog].Term {
				// delete existing conflict logs
				rf.logs = rf.logs[:idxOfNewLog]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
	}
	// 5. update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIdx := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex + len(args.Entries))))
		DPrintf("AppendEntries(%v<-%v) Term(%v<-%v), update commitIdx(%v->%v)\n",
			rf.me, args.LeaderId, rf.currentTerm, args.Term, oldCommitIdx, rf.commitIndex)
	}
	// apply committed command
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf("AppendEntries(%v<-%v) Term(%v<-%v), apply commit msg(%v)\n", 
			rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.logs[rf.lastApplied])
		msg := ApplyMsg{Command:rf.logs[rf.lastApplied].Command, CommandIndex:rf.lastApplied, CommandValid:true, CommandTerm:rf.logs[rf.lastApplied].Term}
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool) {
	DPrintf("sendAppendEntries(%v->%v) args: %s\n", 
		rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("sendAppendEntries(%v->%v) reply %s", rf.me, server, reply)
	} else {
		DPrintf("sendAppendEntries(%v->%v) failed!\n", rf.me, server)
	}
	return ok
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	startTerm := rf.currentTerm
	rf.mu.Unlock()
	DPrintf("heartbeat(%v) start at Term(%v)!\n", rf.me, startTerm)
	for {
		rf.mu.Lock()
		if rf.killed() || !rf.checkRoleTerm(leader, startTerm) {
			DPrintf("heartbeat(%v) end at Term(%v) Role(%v) killed(%v)!\n", 
				rf.me, rf.currentTerm, rf.currentRole, rf.killed())
			rf.mu.Unlock()
			break
		}
		for idxPeer := range rf.peers {
			if idxPeer == rf.me {
				continue
			}
			prevLogIdx := rf.nextIndex[idxPeer] - 1
			go rf.startSendAppendEntries(idxPeer, rf.currentTerm, rf.me, 
				prevLogIdx, rf.logs[prevLogIdx].Term, rf.commitIndex,
				rf.logs[rf.nextIndex[idxPeer]:], false)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(120) * time.Millisecond)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}	

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term:term, Index:index, Command:command})
	rf.persist()
	for idxPeer := range rf.peers {
		if idxPeer == rf.me {
			rf.nextIndex[rf.me] = len(rf.logs)
			rf.matchIndex[rf.me] = len(rf.logs) - 1
			continue
		}
		if index >= rf.nextIndex[idxPeer] {
			prevLogIdx := rf.nextIndex[idxPeer] - 1
			go rf.startSendAppendEntries(idxPeer, rf.currentTerm, rf.me, 
				prevLogIdx, rf.logs[prevLogIdx].Term, rf.commitIndex, 
				rf.logs[prevLogIdx+1:], true)
		}
	}
	return index, term, isLeader
}

func (rf *Raft) startSendAppendEntries(
	idxPeer, term, leaderId, prevLogIdx, prevLogTerm, 
	leaderCommit int, entries []LogEntry, repeat bool) {
	args := AppendEntriesArgs{
		Term: term,
		LeaderId: leaderId,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm: prevLogTerm,
		LeaderCommit: leaderCommit,
		Entries: entries}
	reply := AppendEntriesReply{}
	for {
		ok := rf.sendAppendEntries(idxPeer, &args, &reply)
		rf.mu.Lock()
		if rf.killed() || !rf.checkRoleTerm(leader, args.Term) {
			// either the server is killed or the RPC is outdated
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if !ok {
			time.Sleep(50 * time.Millisecond)	// wait a moment before resend
		} else {
			rf.mu.Lock()
			if reply.Success {
				// RPC succeeded without conflict
				DPrintf("change nextIndex[%v]: %v->%v\n", 
					idxPeer, rf.nextIndex[idxPeer], args.PrevLogIndex + len(args.Entries) + 1)
				rf.nextIndex[idxPeer] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[idxPeer] = args.PrevLogIndex + len(args.Entries)
				rf.mu.Unlock()
				break
			} else if rf.currentTerm < reply.Term {
				// RPC failed due to a new leader, change to folower
				rf.changeToFollower(reply.Term)
				rf.persist()
				rf.mu.Unlock()
				break
			} else {
				// RPC failed due to a conflict LogEntry
				if rf.nextIndex[idxPeer] == args.PrevLogIndex + 1 {
					for rf.logs[rf.nextIndex[idxPeer] - 1].Term == rf.logs[rf.nextIndex[idxPeer] - 2].Term {
						rf.nextIndex[idxPeer]--
					}
					rf.nextIndex[idxPeer]--
				} else {
					repeat = false	// if nextIndex has been changed, then the one changing it will contains new logs
				}
				args.PrevLogIndex = rf.nextIndex[idxPeer] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.Entries = rf.logs[rf.nextIndex[idxPeer]:]
				DPrintf("New args: %s\n", args)
				rf.mu.Unlock()
			}
		}
		if !repeat {
			break
		}
	}
	// change commitIndex if the server is still leader
	rf.mu.Lock()
	if !rf.killed() && rf.checkRoleTerm(leader, term) {
		for idxLog := len(rf.logs) - 1; rf.logs[idxLog].Term == rf.currentTerm && idxLog > rf.commitIndex; idxLog-- {
			cnt := 0
			for idxServer := range rf.peers {
				if rf.matchIndex[idxServer] >= idxLog {
					cnt++
				}
				if cnt > len(rf.peers) / 2 {
					if rf.commitIndex < idxLog {
						rf.commitIndex = idxLog
					}
					break
				}
			}
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			DPrintf("leader %v commit msg (%v) at index (%v)\n", rf.me, rf.logs[rf.lastApplied], rf.lastApplied)
			msg := ApplyMsg{Command:rf.logs[rf.lastApplied].Command, CommandIndex:rf.lastApplied, CommandValid:true, CommandTerm:rf.logs[rf.lastApplied].Term}
			rf.applyCh <- msg
		}
	}
	rf.mu.Unlock()
}

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
	rf 			 := &Raft{}
	rf.peers     = peers
	rf.persister = persister
	rf.me        = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("%v is online now.\n", me)
	rf.currentRole = follower
	rf.currentTerm = 0
	rf.votedFor    = -1
	rf.logs 	   = make([]LogEntry, 1)
	rf.logs[0] 	   = LogEntry{Term:0, Index:0, Command:nil}
	rf.numCompact  = 0
	// volatile data
	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile data on leaders
	rf.nextIndex   = make([]int, len(rf.peers))
	rf.matchIndex  = make([]int, len(rf.peers))
	rf.lastTick    = time.Now()
	rf.applyCh     = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.leaderElection()

	return rf
}
