package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const WaitTimeout = time.Second

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := callForTask()
		action := args.Action
		if action == MAP {
			mapTask(args, mapf)
			callTaskDone(args)
			continue
		}
		if action == REDUCE {
			reduceTask(args, reducef)
			callTaskDone(args)
			continue
		}
		if action == WAIT {
			time.Sleep(WaitTimeout)
			continue
		}
		if action == FINISHED {
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func reduceTask(args *TaskArgs, reducef func(string, []string) string) {
	kvs := make(map[string][]string)

	// searches for mr-{file_index}-{bucket_num}.json
	// with bucket_num being args.Index
	for i := 0; i < args.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d.json", i, args.Index)
		// O_CREATE flag: if the file does not exist, it will be created.
		// O_RDWR flag: file will be opened for both reading and writing.
		// 0644 argument: permission bits for the file.
		file, _ := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		dec := json.NewDecoder(file)
		var kv map[string][]string
		_ = dec.Decode(&kv)
		_ = file.Close()
		for k, v := range kv {
			kvs[k] = append(kvs[k], v...)
		}
	}

	file, _ := os.Create(fmt.Sprintf(("mr-out-%d.txt"), args.Index))
	for k, v := range kvs {
		output := reducef(k, v)
		fmt.Fprintf(file, "%v %v\n", k, output)
	}
	file.Close()
}

func mapTask(args *TaskArgs, mapf func(string, string) []KeyValue) {
	filename := args.FileName
	data := readDataFromFile(filename)
	res := mapf(filename, data)
	writeToIntermediateFiles(args, res)
}

func callTaskDone(args *TaskArgs) {
	taskDone := TaskDone{args.Action, args.Index}
	var reply interface{}
	call("Coordinator.MarkAsDone", &taskDone, &reply)
}

func writeToIntermediateFiles(args *TaskArgs, pairs []KeyValue) {
	// function appends key-value pairs to
	// this type of map: Map[int, Map[string, List[string]]]
	// with bucketNum being key % nReduce
	buckets := make(map[int]map[string][]string)

	// creates nReduce different buckets in buckets
	for i := 0; i < args.NReduce; i++ {
		buckets[i] = make(map[string][]string)
	}

	// finds bucketNum for each key and append values
	for _, kv := range pairs {
		bucketNum := ihash(kv.Key) % args.NReduce
		buckets[bucketNum][kv.Key] = append(buckets[bucketNum][kv.Key],
			kv.Value)
	}

	for i, kvs := range buckets {
		dir, _ := os.Getwd()
		filename := fmt.Sprintf("mr-%d-%d.json", args.Index, i)
		file, _ := os.CreateTemp(dir, filename)
		enc := json.NewEncoder(file)
		_ = enc.Encode(&kvs)
		_ = file.Close()
		_ = os.Rename(file.Name(), filename)
	}
}

func readDataFromFile(name string) string {
	file, err := os.Open(name)
	if err != nil {
		log.Fatalf("cannot open %v", name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", name)
	}
	file.Close()
	return string(content)
}

func callForTask() *TaskArgs {
	var args interface{}
	reply := TaskArgs{}
	call("Coordinator.AssignTask", &args, &reply)
	return &reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
