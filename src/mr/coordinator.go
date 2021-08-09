package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	nReduce     int
	mapDone     bool
	reduceDone  bool
	mapState    []KeyValue
	reduceState map[int]string
	reduceJobs  map[int][]string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

<<<<<<< HEAD:src/mr/master.go
// Tells worker how many reducers there are
func (m *Master) GiveReducerNum(args *NoArgs, reply *ReducerReply) error {
	reply.NReduce = m.nReduce
	return nil
}

// Give worker task
func (m *Master) GiveTask(args *NoArgs, reply *Message) error {

	// not all maps are done
	if !m.mapDone {
		// fmt.Println("before map assign lock")
		m.mu.Lock()
		for i, kv := range m.mapState {
			// if there is an idle map task, assign it to the worker
			if kv.Value == "idle" {
				reply.TaskType = "map"
				reply.TaskNum = i
				reply.Filenames = []string{kv.Key}
				// change the job status to in progress
				m.mapState[i].Value = "in-progress"
				// give the worker 10 seconds for response
				m.mu.Unlock()
				// fmt.Println("after map assign lock")
				go m.checkComplete("map", i)
				return nil
			}
		}
		// not all maps are done and no map job is idle
		// this means some of them are in progress, so we want the worker to wait
		reply.TaskType = "wait"
		m.mu.Unlock()
		return nil

		// fmt.Println("after map assign lock")
	}

	// not all reduce are done
	if !m.reduceDone {
		m.mu.Lock()
		for key, value := range m.reduceState {
			// if there is an idle reduce task, assign it to the worker
			if value == "idle" {
				reply.TaskType = "reduce"
				reply.TaskNum = key
				// change the job status to in progress
				reply.Filenames = m.reduceJobs[key]
				m.reduceState[key] = "in-progress"
				// give the worker 10 seconds for response
				m.mu.Unlock()
				go m.checkComplete("reduce", key)
				return nil
			}
		}
		// all reduce tasks are in progress, the worker should wait
		reply.TaskType = "wait"
		m.mu.Unlock()
		return nil
	}

	return nil
}

// sleep for 10 seconds, if the job is still in progress
// set it back to idle
// so another worker will be pick up this task again
func (m *Master) checkComplete(taskType string, taskNum int) {

	time.Sleep(10 * time.Second)
	m.mu.Lock()
	if taskType == "map" {
		if m.mapState[taskNum].Value == "in-progress" {
			m.mapState[taskNum].Value = "idle"
		}
	} else {
		if m.reduceState[taskNum] == "in-progress" {
			m.reduceState[taskNum] = "idle"
		}
	}
	m.mu.Unlock()
}

// Worker has completed the task given, update state
func (m *Master) TaskCompleted(args *Message, reply *NoArgs) error {
	switch args.TaskType {

	case "map":
		// fmt.Println("before map finish lock")
		m.mu.Lock()
		// set job status to done
		m.mapState[args.TaskNum].Value = "completed"
		// add the file created to the list of reduceJobs
		for _, f := range args.Filenames {
			n := findN(f)
			m.reduceJobs[n] = append(m.reduceJobs[n], f)
		}

		// check: by finishing this map task
		// are all map tasks compeleted
		isCompleted := true
		for _, kv := range m.mapState {
			if kv.Value != "completed" {
				isCompleted = false
				break
			}
		}
		m.mapDone = isCompleted
		m.mu.Unlock()
		// fmt.Println("after map finish lock")
	case "reduce":

		m.mu.Lock()
		// set job status to done
		m.reduceState[args.TaskNum] = "completed"

		// check: by finishing this reduce task
		// are all reduce tasks compeleted
		isCompleted := true
		for _, value := range m.reduceState {
			if value != "completed" {
				isCompleted = false
				break
			}
		}
		if isCompleted {
			m.removeIntermediateFiles()
		}
		m.reduceDone = isCompleted
		m.mu.Unlock()
	}
	return nil
}

// remove all intermediate files
func (m *Master) removeIntermediateFiles() {
	for _, fileNames := range m.reduceJobs {
		for _, fileName := range fileNames {
			os.Remove(fileName)
		}
	}
}

// find the nth reducer num for the file
func findN(fileName string) int {
	n, _ := strconv.Atoi(string(fileName[len(fileName)-1]))
	return n
}
=======
>>>>>>> official/master:src/mr/coordinator.go

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
<<<<<<< HEAD:src/mr/master.go
func (m *Master) Done() bool {

	// Your code here.

	return m.reduceDone
=======
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
>>>>>>> official/master:src/mr/coordinator.go
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	m.nReduce = nReduce

	// initialize map state and reduce state
	m.mapDone = false
	m.reduceDone = false

	m.mapState = []KeyValue{}
	for _, f := range files {
		m.mapState = append(m.mapState, KeyValue{f, "idle"})
	}

	m.reduceState = make(map[int]string)
	m.reduceJobs = make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		m.reduceState[i] = "idle"
		m.reduceJobs[i] = []string{}
	}


	c.server()
	return &c
}
