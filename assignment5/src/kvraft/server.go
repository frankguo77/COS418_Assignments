package raftkv

import (
	"encoding/gob"
	"log"
	"src/labrpc"
	"src/raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Append = 1
	Put    = 2
	Get    = 3
)

type OpRes struct {
	err Err
	val string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      int
	Key       string
	Val       string
	ClientId  int64
	RequestId int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	killed  chan bool

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdata      map[string]string
	waitingMap  map[int]chan OpRes
	lastApplied map[int64]int64
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("get")
	// _, isLeader := kv.rf.GetState()
	// DPrintf("get 1")
	// if !isLeader {
	// 	reply.WrongLeader = true
	// 	return
	// }

	op := Op{
		Type: Get,
		Key:  args.Key,
		ClientId : args.ClientId,
		RequestId : args.RequestId,		
	}

	index, _, isLeader := kv.rf.Start(op)
	DPrintf("PutAppend : index = %d, op = %+v", index, op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}	

	// DPrintf("leader get")
	reply.WrongLeader = false
	err, val := kv.waitApplyOp(index)
	reply.Err = err
	reply.Value = val

}

func (kv *RaftKV) waitApplyOp(index int) (err Err, val string){
	resultChan := make(chan OpRes)
	kv.mu.Lock()
	kv.waitingMap[index] = resultChan
	kv.mu.Unlock()

	// DPrintf("Leader PutAppend : index = %d, op = %+v", index, op)
	var res OpRes
	timeout := time.After(time.Millisecond * 600)
	select {
	case <-timeout:
		err = TimeOut
	case res = <-resultChan:
		err = res.err
		val = res.val
	}

	kv.mu.Lock()
	delete(kv.waitingMap, index)
	kv.mu.Unlock()
	close(resultChan)
	return 
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// _, isLeader := kv.rf.GetState()
	// if !isLeader {
	// 	reply.WrongLeader = true
	// 	return
	// }
	op := Op{}

	if args.Op == "Put" {
		op.Type = Put
	} else {
		op.Type = Append
	}

	kv.mu.Lock()
	if lastApplied, ok := kv.lastApplied[args.ClientId]; ok {
		if args.RequestId <= lastApplied {
			reply.WrongLeader = false
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op.Key = args.Key
	op.Val = args.Value
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId

	DPrintf("Server.PutAppend : args.Value = %s", args.Value)

	index, _, isLeader := kv.rf.Start(op)
	DPrintf("PutAppend : index = %d, op = %+v", index, op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	err, _ := kv.waitApplyOp(index)
	reply.Err = err

	return
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killed)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killed = make(chan bool)
	kv.waitingMap = make(map[int]chan OpRes)
	kv.kvdata = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		var msg raft.ApplyMsg
		for {
			select {
			case <-kv.killed:
				return
			case msg = <-kv.applyCh:
				// _, isLeader := kv.rf.GetState()
				idx := msg.Index
				// DPrintf("Applied Msg: index = %d, isLeader = %t", idx, isLeader)
				// if isLeader {
				// 	DPrintf("Leader Apply : index = %d", idx)
				// }
				// var err Err
				// err = OK
                op, _ := msg.Command.(Op)

                res := OpRes{
					err : OK,
					val : op.Val,
				}

				kv.mu.Lock()
				
				switch op.Type {
				case Append:
					kv.kvdata[op.Key] += op.Val
				case Put:
					kv.kvdata[op.Key] = op.Val
				case Get:
					if val, ok := kv.kvdata[op.Key]; ok {
						res.val = val
					} else {
						res.err = ErrNoKey
					}
				}

				kv.lastApplied[op.ClientId] = op.RequestId

				if ch, ok := kv.waitingMap[idx]; ok {
					// DPrintf("Start write kvch")
					ch <- res
					// DPrintf("End write kvch")
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
