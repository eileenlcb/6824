package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskPhase int
type TaskStatus int

const (
	TaskPhase_Map    TaskPhase = 0
	TaskPhase_Reduce TaskPhase = 1
)

const (
	TaskStatus_New        TaskStatus = 0 //还没有创建
	TaskStatus_Ready      TaskStatus = 1 //进入队列
	TaskStatus_Running    TaskStatus = 2 //已经分配，正在运行
	TaskStatus_Terminated TaskStatus = 3 //运行结束
	TaskStatus_Error      TaskStatus = 4 //运行出错
)

type Task struct {
	FileName string
	Id       int
}

type TaskState struct {
	Status    TaskStatus
	WorkerId  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files      []string
	nReduce    int
	taskPhase  TaskPhase
	taskStates []TaskState
	taskChan   chan Task
	workerSeq  int
	done       bool
	muLock     sync.Mutex
}

func (c *Coordinator) schedule() {
	{
		for !c.done {
			c.scanTaskState()
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Coordinator) scanTaskState() {
	Dprintf("scanTaskState...")
	c.muLock.Lock()
	defer c.muLock.Unlock()

	if c.done {
		return
	}

	allDone := true

	for k, v := range c.taskStates {

	}

}

func (c *Coordinator) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	fmt.Println("Register worker")
	c.workerSeq++
	reply.WorkerId = c.workerSeq
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		taskPhase:  TaskPhase_Map,
		taskStates: make([]TaskState, len(files)),
		workerSeq:  0,
		done:       false,
	}

	if len(files) == 0 {
		log.Fatal("No input files")
	} else if len(files) < nReduce {
		c.taskChan = make(chan Task, len(files))
	} else {
		c.taskChan = make(chan Task, nReduce)
	}
	go c.schedule()
	c.server()
	return &c
}
