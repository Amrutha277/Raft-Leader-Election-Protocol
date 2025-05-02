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
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

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
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers

	state              string // "follower", "candidate", "leader"
	commitIndex        int
	lastApplied        int
	electionResetEvent chan struct{} // to reset election timer
	applyCh            chan ApplyMsg

	// Other internal stuff
	dead       int32 // set by Kill()
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "leader"
	//return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Case 1: Leader's term is outdated
	if args.Term < rf.currentTerm {
		return
	}

	// Case 2: Valid heartbeat or log entry replication
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = "follower"
			rf.persist()
		}

		// Reset election timer on valid AppendEntries
		select {
		case <-rf.electionResetEvent:
		default:
		}
		rf.electionResetEvent <- struct{}{}
		reply.Term = rf.currentTerm
		//reply.Success = true // For now, just accept heartbeats (no log checking yet)
		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}

		// Accept new entries
		index := args.PrevLogIndex + 1
		for i := 0; i < len(args.Entries); i++ {
			if index+i >= len(rf.log) || rf.log[index+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:index+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		rf.persist()

		// Update commit index
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}

		reply.Success = true

	}
}

func (rf *Raft) sendAppendEntries(peer int) {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := make([]LogEntry, len(rf.log[nextIdx:]))
	copy(entries, rf.log[nextIdx:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "follower"
			rf.votedFor = -1
			rf.persist()
			return
		}

		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		} else {
			// backtrack on failure
			if rf.nextIndex[peer] > 1 {
				rf.nextIndex[peer]--
				go rf.sendAppendEntries(peer)
			}
			return
		}

		// attempt commit for any N where a majority has matchIndex >= N
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
		}
	}
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
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If candidate's term is older, reject vote
	if args.Term < rf.currentTerm {
		return
	}

	// If candidate's term is newer, step down
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
	}

	// Check if candidate's log is at least as up-to-date
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionResetEvent <- struct{}{}
		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	//rf.persist()
	rf.persist()
	//isLeader := true
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendAppendEntries(peer)
		}
	}
	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
func (rf *Raft) startElection() {
	//Step 1: Transition to Candidate
	rf.mu.Lock()
	rf.state = "candidate"
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	select {
	case <-rf.electionResetEvent:
	default:
	}
	rf.electionResetEvent <- struct{}{}

	rf.persist()
	//Get log info to send in RequestVote
	//Determines the last log entry's index and term.
	//These will be included in the RequestVoteArgs to help followers decide if the candidate's log is up-to-date.

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	//Initialize Vote Counters
	//Starts with 1 vote (self-vote).
	//finished: number of peers whose votes we've collected (for early exit).
	//done: used to signal when election completes (either win or finish all RPCs).
	votes := 1
	//var mu sync.Mutex
	finished := 1
	done := make(chan bool, 1)

	//Send RequestVote RPCs in Parallel
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		//Loop through all other peers and spawn a goroutine for each:

		go func(peer int) {
			//Prepare and send RequestVote RPC
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, args, &reply) {
				//Re-acquire the lock to safely update Raft state.
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//If the peer reports a newer term, this node is out-of-date and must step down.
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.votedFor = -1
					rf.persist()
					return
				}
				//Count vote
				//Count vote only if still a candidate and terms match.
				var mu sync.Mutex
				mu.Lock()

				if rf.state == "candidate" && reply.Term == rf.currentTerm && reply.VoteGranted {
					votes++
				}
				finished++
				mu.Unlock()

				//If majority reached → become leader and signal success.

				if votes > len(rf.peers)/2 && rf.state == "candidate" {
					rf.state = "leader"

					// initialize matchIndex and nextIndex
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					lastIndex := len(rf.log)
					for i := range rf.peers {
						rf.nextIndex[i] = lastIndex
						rf.matchIndex[i] = 0
					}

					go rf.heartbeatLoop()
					done <- true
				} else if finished == len(rf.peers) {
					done <- true
				}
			}
		}(peer)
	}

	//<-done
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
	}
}
func (rf *Raft) heartbeatLoop() {
	for {
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer != rf.me {
				go rf.sendAppendEntries(peer)
			}
		}
		time.Sleep(100 * time.Millisecond) // heartbeat interval
	}
}

func (rf *Raft) electionTimer() {
	for {
		timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			rf.mu.Lock()
			if rf.state != "leader" {
				go rf.startElection()
			}
			rf.mu.Unlock()
		case <-rf.electionResetEvent:
			timer.Stop()
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			cmd := rf.log[rf.lastApplied].Command
			applyMsg := ApplyMsg{Index: rf.lastApplied, Command: cmd}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	rf := &Raft{
		peers:              peers,
		persister:          persister,
		me:                 me,
		votedFor:           -1,
		state:              "follower",
		log:                []LogEntry{{}}, // log[0] is dummy entry
		electionResetEvent: make(chan struct{}, 1),
		applyCh:            applyCh,
	}

	//rf.peers = peers
	//rf.persister = persister
	//rf.me = me

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.electionTimer()
	go rf.applier()

	return rf
}
