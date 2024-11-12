package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files []string

	// runningFiles   []string
	remainingFiles []string

	currentRunningPhase TaskType

	nReduce int

	tasks map[int]Task // key is the worker id

	intermediateFiles map[int][]IntermediateFile
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
	reply.Id = len(c.tasks) + 1
	reply.NReduce = c.nReduce
	// reply.CurrentStatus = "idle"

	// devo registrar o Id aqui?
	// c.runningTasks[reply.Id] = Task{}
	return nil
}

func (c *Coordinator) AskForATask(args *WorkerServer, replyTask *Task) error {
	fmt.Println("AskForATask, currentRunningPhase: ", c.currentRunningPhase)
	c.checkFailedTasks()

	// @TODO add locking in `c`, because each RPC runs on a thread

	if c.currentRunningPhase == Map {
		if len(c.remainingFiles) == 0 {
			// here we reply empty for the worker, and then we change the runningPhase to reduce,
			// next time worker asks for a task, we will pass a reduce task
			replyTask.TaskType = Empty

			if c.countRemainingTasks() == 0 {
				c.currentRunningPhase = Reduce
			}
		} else {
			currentFile := c.remainingFiles[0]
			c.remainingFiles = c.remainingFiles[1:] // pop from remaining files

			replyTask.Filename = currentFile
			replyTask.TaskType = Map
			replyTask.StartedAt = time.Now()

			// here, we save the task into the coordinator
			c.tasks[args.Id] = *replyTask
		}
	} else if c.currentRunningPhase == Reduce {
		if len(c.intermediateFiles) == 0 {
			replyTask.TaskType = Empty
		} else {
			// get the first key from the intermediate files
			for reduceId, _ := range c.intermediateFiles {
				replyTask.IntermediateFiles = c.intermediateFiles[reduceId]

				// here we clean the intermediate files from current queue
				c.intermediateFiles[reduceId] = nil
				break
			}

			replyTask.TaskType = Reduce
			replyTask.StartedAt = time.Now()

			// saving the task into the coordinator
			c.tasks[args.Id] = *replyTask
		}
	}
	return nil
}

func (c *Coordinator) TaskSuccessful(args *WorkerServer, reply *struct{}) error {
	task, ok := c.tasks[args.Id]
	fmt.Printf("\nTaskSuccessful task %#v\n", task)
	fmt.Printf("\nTaskSuccessful args %#v\n", args)

	if (task.TaskStatus == Aborted) || !ok {
		fmt.Println("WARNING receiving success result of a aborted task")
		return nil
	}

	// add intermediate files so they can be used in reduce phase,
	// already agrupping them by reduce key
	if task.TaskType == Map {
		for _, intermediateFile := range args.IntermediateFiles {
			if c.intermediateFiles[intermediateFile.ReduceId] == nil {
				c.intermediateFiles[intermediateFile.ReduceId] = []IntermediateFile{}
			}

			c.intermediateFiles[intermediateFile.ReduceId] = append(c.intermediateFiles[intermediateFile.ReduceId], intermediateFile)
		}
	}

	// task.TaskStatus = Finished
	// remove tasks from running tasks
	delete(c.tasks, args.Id)

	return nil
}

func (c *Coordinator) checkFailedTasks() {
	fmt.Println("checkFailedTasks runningTasks: ", len(c.tasks))

	for workerId, task := range c.tasks {
		fmt.Printf("checkFailedTasks task %#v\n", task)

		if time.Now().Sub(task.StartedAt).Seconds() > 10 {
			// in this case we need to remove this task and return the file to the queue
			fmt.Println("task running for more than 10 secods", task, workerId)
			c.removeFailedTask(workerId, task)
		}
	}
}

func (c *Coordinator) removeFailedTask(workerId int, task Task) {
	file := task.Filename
	c.remainingFiles = append(c.remainingFiles, file)

	delete(c.tasks, workerId)
}

func (c *Coordinator) countRunningTasks() int {
	count := 0
	for _, task := range c.tasks {
		if task.TaskStatus == Started {
			count++
		}
	}

	return count
}

func (c *Coordinator) countRemainingTasks() int {
	return len(c.remainingFiles) + c.countRunningTasks()
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

	fmt.Println("Done: currentRunningPhase: ", c.currentRunningPhase)
	fmt.Println("Done: missing: ", c.countRemainingTasks())

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
	// c.runningFiles = []string{}

	c.currentRunningPhase = Map
	c.nReduce = nReduce

	c.intermediateFiles = make(map[int][]IntermediateFile)

	c.tasks = make(map[int]Task, 0)

	// Your code here.

	c.server()
	return &c
}
