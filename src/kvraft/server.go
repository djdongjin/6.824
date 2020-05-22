package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name  string
	Key   string
	Value string
}

func (op Op) String() string {
	return fmt.Sprintf("%v: (%v,%v)", op.Name, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data	   map[string]string
	idx2op	   map[int]Op
	idx2term   map[int]int
	idx2resend map[int]bool
}


func (kv *KVServer) receiveMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("Receive msg from Raft: %v\n", msg)
		if msg.CommandValid {
			op := msg.Command.(Op)
			term := msg.CommandTerm
			index := msg.CommandIndex
			kv.mu.Lock()
			
			val, ok := kv.data[op.Key]
			if !(kv.idx2resend[index] && kv.idx2term[index] == term) {
				if op.Name == "Put" {
					kv.data[op.Key] = op.Value
				} else if op.Name == "Append" {
					if !ok {
						val = ""
					}
					kv.data[op.Key] = val + op.Value
				}
			}
			kv.idx2op[index] = op
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) waitOpFinished(index int) Op {
	var replyOp Op
	var ok bool
	for {
		kv.mu.Lock()
		replyOp, ok = kv.idx2op[index]
		if ok {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return replyOp
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Name:"Get", Key:args.Key, Value:""}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.idx2resend[index] = false
	kv.idx2term[index] = term
	kv.mu.Unlock()
	replyOp := kv.waitOpFinished(index)
	kv.mu.Lock()
	if replyOp != op {
		kv.idx2resend[index] = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = ErrNoKey
		val, ok := kv.data[op.Key]
		if ok {
			reply.Err = OK
			reply.Value = val
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Name:args.Op, Key:args.Key, Value:args.Value}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.idx2resend[index] = false
	kv.idx2term[index] = term
	kv.mu.Unlock()
	replyOp := kv.waitOpFinished(index)
	kv.mu.Lock()
	if replyOp != op {
		kv.idx2resend[index] = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.data = make(map[string]string)
	kv.idx2op = make(map[int]Op)
	kv.idx2term = make(map[int]int)
	kv.idx2resend = make(map[int]bool)
	go kv.receiveMsg()

	return kv
}
