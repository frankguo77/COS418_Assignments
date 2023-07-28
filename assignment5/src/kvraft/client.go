package raftkv

import (
	"crypto/rand"
	"math/big"
	"src/labrpc"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int64
	leaderId  int32
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	leaderId := atomic.LoadInt32(&ck.leaderId)
	// You will have to modify this function.
	args := &GetArgs{
		Key:       key,
		ClientId:  reqId,
		RequestId: ck.clientId,
	}

	reply := &GetReply{}
	if ck.servers[leaderId].Call("RaftKV.Get", args, reply) {
		if reply.WrongLeader == false && reply.Err == OK {
			return reply.Value
		}
	}

	for {
		for i, svr := range ck.servers {
			reply := &GetReply{}
			if svr.Call("RaftKV.Get", args, reply) {
				if reply.WrongLeader == false && reply.Err == OK {
					atomic.StoreInt32(&ck.leaderId, int32(i))
					return reply.Value
				}
			}
		}
		// break
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	leaderId := atomic.LoadInt32(&ck.leaderId)
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	reply := &PutAppendReply{}
	if ck.servers[leaderId].Call("RaftKV.PutAppend", args, reply) {
		if reply.WrongLeader == false && reply.Err == OK {
			return
		}
	}

	for {
		for i, svr := range ck.servers {
			reply := &PutAppendReply{}
			DPrintf("call Server[%d].PutAppend !", i)
			if svr.Call("RaftKV.PutAppend", args, reply) {
				DPrintf("Clerk.PutAppend : %+v", reply)
				if reply.WrongLeader == false && reply.Err == OK {
					atomic.StoreInt32(&ck.leaderId, int32(i))
					// if reply.Err == OK {
					DPrintf("Clerk.PutAppend : OK")
					return
				}
			}
		}
		// break
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Clerk.Put : value = %s", value)
	ck.PutAppend(key, value, "Put")
	DPrintf("Clerk.Put : OK")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Clerk.Append : value = %s", value)
	ck.PutAppend(key, value, "Append")
	DPrintf("Clerk.Append : OK")
}
