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
	SID   int
	UID	  int64
}

type Status string
const (
	fail Status = "fail"
	succ Status = "succ"
	send Status = "send"
)

func (op Op) String() string {
	return fmt.Sprintf("(%v,%v) (%v,%v)", op.Name, op.SID, op.Key, op.Value)
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
	sid2status map[int64]map[int]Status	// uid->sid->status
	idx2uid	   map[int]int64			// index->uid
	idx2sid	   map[int]int				// index->sid
	getVal     map[int64]map[int]string // uid->sid->value
}


func (kv *KVServer) receiveMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("%v Receive msg from Raft: %v\n", kv.me, msg)
		if msg.CommandValid {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			kv.mu.Lock()
			DPrintf("%v To access (UID,SID):(%v,%v)\n", kv.me, op.UID, op.SID)
			if _, ok := kv.sid2status[op.UID]; !ok {
				kv.sid2status[op.UID] = make(map[int]Status)
			}
			
			if status, ok := kv.sid2status[op.UID][op.SID]; !ok || status != succ {
				kv.sid2status[op.UID][op.SID] = succ
				DPrintf("%v Commit Op %s \n", kv.me, op)
				prevUID, ok2 := kv.idx2uid[index]
				prevSID, ok3 := kv.idx2sid[index]
				if ok2 && prevUID != op.UID || 
				   ok3 && prevSID != op.SID {
					kv.sid2status[prevUID][prevSID] = fail
				}
				kv.idx2uid[index] = op.UID
				kv.idx2sid[index] = op.SID
				val, ok5 := kv.data[op.Key]
				if op.Name == "Put" {
					kv.data[op.Key] = op.Value
				} else if op.Name == "Append" {
					if !ok5 {
						val = ""
					}
					kv.data[op.Key] = val + op.Value
				} else {
					if _, ok6 := kv.getVal[op.UID]; !ok6 {
						kv.getVal[op.UID] = make(map[int]string)
					}
					if _, ok6 := kv.data[op.Key]; ok6 {
						kv.getVal[op.UID][op.SID] = kv.data[op.Key]
					} else {
						kv.getVal[op.UID][op.SID] = ""
					}
				}
			}
			DPrintf("%v Status of (UID,SID):(%v,%v): %v\n", kv.me, op.UID, op.SID, kv.sid2status[op.UID][op.SID])
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) waitOpFinished(uid int64, sid int) Status {
	i := 0
	for {
		kv.mu.Lock()
		if kv.sid2status[uid][sid] != send {
			kv.mu.Unlock()
			return kv.sid2status[uid][sid]
		}
		kv.mu.Unlock()
		i++
		if i > 70 {
			return fail
		}
		time.Sleep(10 * time.Millisecond)
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Name:"Get", Key:args.Key, Value:"", SID:args.SID, UID:args.UID}
	DPrintf("%v Op %s\n", kv.me, op)
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	var status Status
	kv.mu.Lock()
	if _, ok := kv.sid2status[args.UID]; !ok {
		kv.sid2status[args.UID] = make(map[int]Status)
	}
	if s, ok := kv.sid2status[args.UID][args.SID]; !ok || s != succ {
		kv.sid2status[args.UID][args.SID] = send
		kv.idx2sid[index] = args.SID 
		kv.idx2uid[index] = args.UID
		kv.mu.Unlock()
		status = kv.waitOpFinished(args.UID, args.SID)
		kv.mu.Lock()
	} else {
		status = succ
	}
	if status == fail {
		// return ErrWrongLeader
	} else {
		reply.Value = kv.getVal[args.UID][args.SID]
		if reply.Value != "" {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		DPrintf("%v Return get val: (Key,UID,SID,status,err):(%v,%v,%v,%v,%v)\n%v\n", 
			kv.me, args.Key, args.UID, args.SID, status, reply.Err,
			reply.Value)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Name:args.Op, Key:args.Key, Value:args.Value, SID:args.SID, UID:args.UID}
	DPrintf("%v Op %s\n", kv.me, op)
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	var status Status
	kv.mu.Lock()
	if _, ok := kv.sid2status[args.UID]; !ok {
		kv.sid2status[args.UID] = make(map[int]Status)
	}
	if s, ok := kv.sid2status[args.UID][args.SID]; !ok || s != succ {
		kv.sid2status[args.UID][args.SID] = send
		kv.idx2sid[index] = args.SID 
		kv.idx2uid[index] = args.UID
		kv.mu.Unlock()
		status = kv.waitOpFinished(args.UID, args.SID)
		kv.mu.Lock()
	} else {
		status = succ
	}
	if status == fail {
		// return ErrWrongLeader
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
	kv.idx2sid = make(map[int]int)
	kv.idx2uid = make(map[int]int64)
	kv.sid2status = make(map[int64]map[int]Status)
	kv.getVal = make(map[int64]map[int]string)
	go kv.receiveMsg()

	return kv
}
