package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
		task, err := w.getTask()
		if err != nil {
			Dprintf("get task failed")
			continue
		}
		if !task.Alive {
			Dprintf("task is not alive,exit")
			return
		}
		w.doTask(*task)
	}
}

func (w *worker) doTask(task Task) {
	switch task.Phase {
	case TaskPhase_Map:
		w.doMapTask(task)
	case TaskPhase_Reduce:
		w.doReduceTask(task)
	default:
		panic(fmt.Sprintf("unknown task phase:%v", task.Phase))
	}

}

func (w *worker) getReduceName(mapId, partitionId int) string {
	return fmt.Sprintf("mr-kv-%d-%d", mapId, partitionId)
}

func (w *worker) getMergeName(partitionId int) string {
	return fmt.Sprintf("mr-out-%d", partitionId)
}

func (w *worker) doMapTask(task Task) {
	Dprintf("do map task %v", task)
	cont, err := os.ReadFile(task.FileName)
	if err != nil {
		Dprintf("read file failed:%v", err)
		return
	}

	kvs := w.mapF(task.FileName, string(cont))
	partions := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partions[pid] = append(partions[pid], kv)
	}

	for k, v := range partions {
		//to get the file name
		fileName := w.getReduceName(task.Seq, k)
		file, err := os.Create(fileName)
		if err != nil {
			Dprintf("create file-%v fail in doMapTask. %v", fileName, err)
			w.reportTask(task, false)
			return
		}
		//json encoder simply writes the json representation of the value to the stream
		encoder := json.NewEncoder(file)
		for _, kv := range v {
			if err := encoder.Encode(&kv); err != nil {
				Dprintf("encode  kvs to file-%v  fail in doMapTask. %v", fileName, err)
				w.reportTask(task, false)
			}
		}
		if err := file.Close(); err != nil {
			Dprintf("close file fail in doMapTask. %v", err)
			w.reportTask(task, false)
		}
	}
	w.reportTask(task, true)
}

func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)

	for i := 0; i < task.NMap; i++ {
		fileName := w.getReduceName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			Dprintf("open file-%v fail in doReduceTask. %v", fileName, err)
			w.reportTask(task, false)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0)
	for k, v := range maps {
		len := w.reduceF(k, v)
		res = append(res, fmt.Sprintf("%v %v\n", k, len))
	}

	fileName := w.getMergeName(task.Seq)
	if err := os.WriteFile(fileName, []byte(fmt.Sprintf("%v", res)), 0666); err != nil {
		Dprintf("write file-%v fail in doReduceTask. %v", fileName, err)
		w.reportTask(task, false)
	}

	w.reportTask(task, true)
}

func (w *worker) getTask() (*Task, error) {
	args := TaskArgs{WorkerId: w.workerId}
	reply := TaskReply{}

	if err := call("Coordinator.GetOneTask", &args, &reply); !err {
		return nil, errors.New("get task failed")
	}
	Dprintf("get task %v", reply.Task)
	return reply.Task, nil
}

func (w *worker) reportTask(task Task, done bool) {
	args := reportTaskArgs{
		WorkId: w.workerId,
		Phase:  task.Phase,
		Seq:    task.Seq,
		Done:   done,
	}
	reply := reportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		Dprintf("report task failed")
	}
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}

	
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
