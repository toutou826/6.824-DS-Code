package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	mu            sync.Mutex
	commandNumber int
	lastLeaderId  int
	id            int64
	// You will have to modify this struct.
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
	ck.commandNumber = 0
	ck.id = nrand()
	return ck
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
	args := GetArgs{}
	args.ClientId = ck.id
	args.Key = key

	args.CommandNumber = ck.commandNumber
	ck.commandNumber++

	for {
		reply := Reply{}
		ok := ck.servers[ck.lastLeaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.lastLeaderId = (ck.lastLeaderId + 1) % len(ck.servers)
			continue
		}

		// Success
		if reply.Err == OK {
			return reply.Value
		}
		return ""
	}
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
	args := PutAppendArgs{}
	args.ClientId = ck.id
	args.Key = key
	args.Value = value
	args.Op = op

	args.CommandNumber = ck.commandNumber
	ck.commandNumber++

	for {
		reply := Reply{}
		ok := ck.servers[ck.lastLeaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.lastLeaderId = (ck.lastLeaderId + 1) % len(ck.servers)
			continue
		}
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
