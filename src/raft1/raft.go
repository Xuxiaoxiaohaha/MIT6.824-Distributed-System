package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Command interface{} // 日志条目包含的命令
	Term    int         // 日志条目的任期号
}

type PeerState int

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// state
	state PeerState // 跟随者、候选者、领导者
	// Persistent state on all servers
	currentTerm int        // 任期号
	voteFor     int        // 当前任期内投票给的候选人ID
	log         []LogEntry // 日志
	// Volatile state on all servers
	commitIndex int // 已被集群大多数节点提交的日志条目索引
	lastApplied int // 此节点已应用到状态机的日志条目索引（<=commitIndex）
	// Volatile state on leaders
	nextIndex  []int // 乐观的猜测：它表示领导者下一次准备发给某个跟随者的日志条目的索引
	matchIndex []int // 保守的事实：它表示领导者确认已经成功复制到某个跟随者上的日志条目的最高索引
	//
	resetTimerCh chan bool // 重置选举定时器的通道

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("S%d (任期 %d) 收到来自 S%d (任期 %d) 的投票请求", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("S%d (任期 %d) Refuse 来自 S%d (任期 %d) 的投票请求（原因1）", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor == -1) {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("S%d (任期 %d) Accept 来自 S%d (任期 %d) 的投票请求", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else { // 在当前任期内已经投过票
		reply.VoteGranted = false
		DPrintf("S%d (任期 %d) Refuse 来自 S%d (任期 %d) 的投票请求（原因2）", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
	reply.Term = rf.currentTerm

}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		DPrintf("S%d (任期 %d) 收到 S%d（任期 %d）的心跳(情况1)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
	} else if args.Term == rf.currentTerm {
		DPrintf("S%d (任期 %d) 收到 S%d（任期 %d）的心跳(情况2)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = true
		rf.resetTimerCh <- true
	} else {
		DPrintf("S%d (任期 %d) 收到 S%d（任期 %d）的心跳(情况3)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		rf.currentTerm = args.Term
		rf.resetTimerCh <- true
		rf.voteFor = -1
		rf.state = Follower
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (3B).

	return index, term, isLeader
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
	DPrintf("Raft instance %d is being killed.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == Follower || state == Candidate {
			// 设置一个初始的、随机的选举超时
			timer := time.NewTimer(time.Duration(200+(rand.Int63()%200)) * time.Millisecond)
			select {
			// 情况一：定时器超时了
			case <-timer.C:
				// 超时发生，发起选举
				rf.startElection()
				// 为下一轮选举重置定时器
				timer.Reset(time.Duration(200+(rand.Int63()%200)) * time.Millisecond)

			// 情况二：收到了重置信号（比如，从心跳RPC处理器传来）
			case <-rf.resetTimerCh:
				// 收到了心跳，重置定时器
				// 先停止旧的定时器，再重置，这是安全的做法
				if !timer.Stop() {
					// 如果 Stop() 返回 false，说明定时器已经触发了，
					// 需要从其通道中排空值，防止下次select误读
					<-timer.C
				}
				timer.Reset(time.Duration(200+(rand.Int63()%200)) * time.Millisecond)
			}
		} else {
			// 发送添加条目的 RPC 给其他所有服务器
			DPrintf("领导者 S%d (任期 %d) 发送心跳", rf.me, rf.currentTerm)
			for index := range rf.peers {
				if index != rf.me {
					go func(serverIndex int) {
						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
						var reply AppendEntriesReply
						ok := rf.sendAppendEntries(serverIndex, &args, &reply)
						rf.mu.Lock()
						if ok {
							// DPrintf("领导者 S%d (任期 %d) 收到心跳反馈 %t,%d", rf.me, rf.currentTerm, reply.Success, reply.Term)
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.voteFor = -1
								DPrintf("领导者 S%d (任期 %d) 变成跟随者", rf.me, rf.currentTerm)
							}
						}
						rf.mu.Unlock()
					}(index)
				}
			}
			time.Sleep(110 * time.Millisecond) // 心跳间隔 110ms
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.voteFor = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.resetTimerCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1  // 1.自增当前的任期号
	rf.voteFor = rf.me   // 2.给自己投票
	rf.state = Candidate // 3.候选者
	rf.mu.Unlock()
	DPrintf("S%d 在任期 %d 开始选举", rf.me, rf.currentTerm)
	biggerItemCh := make(chan bool)
	voteCh := make(chan bool, len(rf.peers)-1)
	// 4.发送请求投票的 RPC 给其他所有服务器
	for index := range rf.peers {
		if index != rf.me {
			go func(serverIndex int) {
				DPrintf("S%d 在任期 %d 向 S%d 发起拉票", rf.me, rf.currentTerm, index)
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
				var reply RequestVoteReply
				ok := rf.sendRequestVote(serverIndex, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Term > rf.currentTerm {
					biggerItemCh <- true
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.state = Follower
				}
				DPrintf("S%d 在任期 %d 收到 S%d 的投票结果 %t-%d-%t", rf.me, rf.currentTerm, index, ok, reply.Term, reply.VoteGranted)
				voteCh <- (ok && reply.VoteGranted) // 将投票结果发送到channel
			}(index)
		}
	}
	// 开始收集选票
	voteNum := 1 // 自己的票
	// 记录已经返回结果的RPC数量
	responses := 0
	// 循环收集，直到获得多数票或所有RPC都返回
Loop:
	for {
		if voteNum > len(rf.peers)/2 {
			break
		}
		if responses == len(rf.peers)-1 {
			break
		}

		select {
		case VoteGranted := <-voteCh:
			if VoteGranted {
				voteNum += 1
				DPrintf("S%d (任期 %d) 票数: %d, 人数:%d", rf.me, rf.currentTerm, voteNum, len(rf.peers))
			}
		case <-biggerItemCh:
			// 正确跳出外层 for
			break Loop
		case <-time.After(500 * time.Millisecond):
			break Loop
		}

		responses += 1
	}
	rf.mu.Lock()
	if voteNum > len(rf.peers)/2 && rf.state == Candidate {
		rf.state = Leader
		DPrintf("S%d (任期 %d) Success 当选领导者", rf.me, rf.currentTerm)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
	} else {
		rf.state = Follower
		DPrintf("S%d (任期 %d) Lose 当选领导者", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}
