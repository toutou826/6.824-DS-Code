package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true
const HANDLERTIMEOUT = 200

type OpType int

const (
	GET    OpType = iota
	PUT    OpType = iota
	APPEND OpType = iota
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type          OpType
	CommandNumber int
	ClientId      int64
	Key           string
	Value         string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied   map[int64]int
	responseChMap map[int]chan Update
	database      map[string]string
}

type Update struct {
	CommandNumber int
	ClientId      int64
	Err           Err
	Value         string
}

func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	// Your code here.

	op := Op{}
	op.Type = GET
	op.ClientId = args.ClientId
	op.CommandNumber = args.CommandNumber
	op.Key = args.Key

	kv.completeAgreement(op, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *Reply) {
	// Your code here.

	op := Op{}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	op.ClientId = args.ClientId
	op.CommandNumber = args.CommandNumber
	op.Key = args.Key
	op.Value = args.Value

	kv.completeAgreement(op, reply)
}

func (kv *KVServer) completeAgreement(op Op, reply *Reply) {
	// send command to raft
	index, _, isLeader := kv.rf.Start(op)

	// not a leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// create channel for applier to send result to
	kv.mu.Lock()
	if _, ok := kv.responseChMap[index]; !ok {
		kv.responseChMap[index] = make(chan Update, 1)
	}
	ch := kv.responseChMap[index]
	kv.mu.Unlock()

	select {
	case result := <-ch:
		// check if command index are equal and command is the same
		if result.ClientId == op.ClientId && result.CommandNumber == op.CommandNumber {
			// success, create reply
			reply.Err = result.Err
			if op.Type == GET && result.Err != ErrNoKey {
				reply.Value = result.Value
			}

		} else {
			//oudated
			reply.Err = ErrWrongLeader
		}

	// timeout
	case <-time.After(HANDLERTIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
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

func (kv *KVServer) applier() {

	// pull messages from applyCH
	for msg := range kv.applyCh {

		kv.mu.Lock()

		if kv.killed() {
			kv.mu.Unlock()
			break
		}

		if msg.CommandValid {

			op := msg.Command.(Op)

			// object for notifying handler
			update := Update{}
			update.ClientId = op.ClientId
			update.CommandNumber = op.CommandNumber

			update.Err = OK
			// update database
			switch op.Type {
			case GET:
				val, ok := kv.database[op.Key]
				if !ok {
					update.Err = ErrNoKey
				} else {
					update.Value = val
				}
				kv.lastApplied[op.ClientId] = op.CommandNumber
			case PUT:
				// skip duplicate request
				if kv.notDuplicate(op.ClientId, op.CommandNumber) {
					kv.database[op.Key] = op.Value
					kv.lastApplied[op.ClientId] = op.CommandNumber
				}

			case APPEND:
				// skip duplicate request
				if kv.notDuplicate(op.ClientId, op.CommandNumber) {
					if _, ok := kv.database[op.Key]; !ok {
						kv.database[op.Key] = op.Value
					} else {
						kv.database[op.Key] += op.Value
					}
					kv.lastApplied[op.ClientId] = op.CommandNumber
				}
			}

			// send update to channel to handler to respond to client
			if ch, ok := kv.responseChMap[msg.CommandIndex]; ok {
				ch <- update
			}

			// take snapshot if log grows too big
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.saveSnapshot(msg.CommandIndex)
			}

		} else if msg.SnapshotValid {
			// readsnapshot
			kv.readSnapshot(msg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastApplied map[int64]int
	var database map[string]string

	if d.Decode(&lastApplied) != nil || d.Decode(&database) != nil {
		DPrintf("Decode Error")
	} else {
		kv.lastApplied = lastApplied
		kv.database = database
	}
}

func (kv *KVServer) saveSnapshot(lastReceivedLogIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// DPrintf(fmt.Sprint(kv.database))
	e.Encode(kv.lastApplied)
	e.Encode(kv.database)

	kv.rf.Snapshot(lastReceivedLogIndex, w.Bytes())
}

func (kv *KVServer) notDuplicate(clientId int64, commandNumber int) bool {
	lastIndex, ok := kv.lastApplied[clientId]
	if !ok || commandNumber > lastIndex {
		return true
	}
	return false
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
	kv.persister = persister

	// You may need initialization code here.

	// log index to channel map
	kv.lastApplied = make(map[int64]int)
	kv.responseChMap = make(map[int]chan Update)
	// database
	kv.database = make(map[string]string)

	go kv.applier()

	return kv
}
