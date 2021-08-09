package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	nReducer := CallNReducer()

	// keep asking for task until master ends
	for reply, status := CallAsk(); status; reply, status = CallAsk() {
		switch reply.TaskType {

		case "map":
			doMap(reply, nReducer, mapf)

		case "reduce":
			doReduce(reply, reducef)

		case "wait":
			time.Sleep(20 * time.Millisecond)

		}
		time.Sleep(10 * time.Millisecond)
	}

}
func doMap(reply *Message, nReducer int, mapf func(string, string) []KeyValue) {
	// read file
	file, err := os.Open(reply.Filenames[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filenames[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filenames[0])
	}
	file.Close()

	kva := mapf(reply.Filenames[0], string(content))

	// map of reducerNum to encoder
	nToFile := make(map[int]*json.Encoder)

	// array of filenames of temp files, used send to master
	filenames := []string{}

	files := []*os.File{}

	for _, kv := range kva {
		// determine which reducer to assign to
		nth := ihash(kv.Key) % nReducer

		// check if we have already created the temp file
		enc, ok := nToFile[nth]

		// create it if we had not
		if !ok {
			oname := "mr-" + fmt.Sprint(reply.TaskNum) + "-" + fmt.Sprint(nth)
			filenames = append(filenames, oname)
			ofile, _ := os.Create(oname)
			files = append(files, ofile)
			enc = json.NewEncoder(ofile)
			nToFile[nth] = enc
		}
		enc.Encode(kv)
	}

	// close all files
	for _, f := range files {
		f.Close()
	}

	// Tell master work is completed

	completedArgs := Message{}
	completedArgs.TaskType = "map"
	completedArgs.TaskNum = reply.TaskNum
	completedArgs.Filenames = filenames

	completedReply := NoArgs{}

	call("Master.TaskCompleted", &completedArgs, &completedReply)
}

func doReduce(reply *Message, reducef func(string, []string) string) {
	// decode the temp file into a slice of keyValue pairs

	var kva []KeyValue

	// for each file, create new decoder
	for _, f := range reply.Filenames {
		ofile, _ := os.Open(f)
		dec := json.NewDecoder(ofile)
		//for each keyvalue pairs in the file, append to kva
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		ofile.Close()
	}

	sort.Sort(ByKey(kva))

	// create temp output file
	tempFile, _ := ioutil.TempFile(".", "")

	// call Reduce on each distinct key in kva
	// and print the result to temp file
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// writes to temp file
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// rename the temporary file with the correct name
	oname := "mr-out-" + fmt.Sprint(reply.TaskNum)
	os.Rename(tempFile.Name(), oname)
	tempFile.Close()

	// Tell the master the work is done
	completedArgs := Message{}
	completedArgs.TaskType = "reduce"
	completedArgs.TaskNum = reply.TaskNum
	completedArgs.Filenames = []string{oname}

	completedReply := NoArgs{}

	call("Master.TaskCompleted", &completedArgs, &completedReply)
}

// ask master nReducer num
func CallNReducer() int {
	args := NoArgs{}
	reply := ReducerReply{}

	call("Master.GiveReducerNum", &args, &reply)
	return reply.NReduce
}

// send an RPC to the master asking for a task
func CallAsk() (*Message, bool) {

	args := NoArgs{}

	reply := Message{}

	hasEnd := call("Master.GiveTask", &args, &reply)

	return &reply, hasEnd

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// the master has ended
		// log.Print("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
