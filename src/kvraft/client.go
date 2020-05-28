package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

var clerkID int = 0
var clerkMu sync.Mutex 

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
	mu        sync.Mutex
	sids      map[int]bool
	uid		  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.curLeader = 0
	ck.sids = make(map[int]bool)
	clerkMu.Lock()
	ck.uid = clerkID
	clerkID++
	clerkMu.Unlock()
	
	return ck
}

func (ck *Clerk) getSID() int64 {
	return nrand()
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ok    := false
	args  := GetArgs{Key:key, SID:ck.getSID(), UID:ck.uid}
	reply := GetReply{}
	leader := ck.curLeader
	ck.mu.Unlock()
	for {
		ok = ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
		ck.mu.Lock()
		if ck.curLeader == leader {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}
		leader = ck.curLeader
		ck.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	// reply.Value = "" if reply.Err == ErrNoKey
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ok := false
	args := PutAppendArgs{Key:key, Value:value, Op:op, SID:ck.getSID(), UID:ck.uid}
	reply := PutAppendReply{}
	leader := ck.curLeader
	ck.mu.Unlock()
	for {
		ok = ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
		ck.mu.Lock()
		if ck.curLeader == leader {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}
		leader = ck.curLeader
		ck.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
