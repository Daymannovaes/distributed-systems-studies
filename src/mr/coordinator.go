package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	files []string

	runningFiles   []string
	remainingFiles []string

	runningType string

	nReduce int

	workers []WorkerServer
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *struct{}, reply *WorkerServer) error {
	worker := WorkerServer{
		Id: len(c.workers) + 1,
	}

	c.workers = append(c.workers, worker)

	reply.Id = worker.Id

	return nil
}

func (c *Coordinator) AskForAJob(args *struct{}, reply *Task) error {
	reply.NReduce = c.nReduce

	if c.runningType == "map" {
		if len(c.remainingFiles) == 0 {
			c.runningType = "reduce"
			reply.TaskType = "reduce"

			return nil
		}

		currentFile := c.remainingFiles[0]
		c.remainingFiles = c.remainingFiles[1:]
		c.runningFiles = append(c.runningFiles, currentFile)

		reply.TaskType = "map"
		reply.Filename = currentFile
	} else {
		// dunno
	}
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
	c := Coordinator{}

	c.files = files
	c.remainingFiles = files
	c.runningFiles = []string{}

	c.runningType = "map"
	c.nReduce = nReduce

	c.workers = []WorkerServer{}

	// Your code here.

	c.server()
	return &c
}
