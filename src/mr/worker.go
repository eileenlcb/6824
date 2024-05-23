package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId int
	mapF     func(string, string) []KeyValue
	reduceF  func(string, []string) string
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
	worker := worker{
		mapF:    mapf,
		reduceF: reducef,
	}

	worker.register()
	worker.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *worker) run() {
	Dprintf("run")
	for {
		//todo to add the task
		task, err := w.getTask()
	}
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	// just return the workerId
	// type RegisterReply struct {
	// 	WorkerId int
	// }
	ok := call("Coordinator.RegWorker", &args, &reply)
	if ok {
		w.workerId = reply.WorkerId

	} else {
		fmt.Println("register failed!")
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
