package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandNumber int
	ClientId      int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandNumber int
	ClientId      int64
}

type Reply struct {
	Err   Err
	Value string
}
