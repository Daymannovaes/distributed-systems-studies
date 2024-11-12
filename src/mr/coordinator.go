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

	runningType string

	nReduce int

	tasks map[int]Task // key is the worker id
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
	fmt.Println("\n\naskforatask")
	c.checkFailedTasks()

	if c.runningType == "map" {
		if len(c.remainingFiles) == 0 {
			replyTask.TaskType = Empty
		} else {
			currentFile := c.remainingFiles[0]
			c.remainingFiles = c.remainingFiles[1:] // pop from remaining files
			// c.runningFiles = append(c.runningFiles, currentFile)

			replyTask.Filename = currentFile
			replyTask.TaskType = Map
			replyTask.StartedAt = time.Now()

			// here, we save the task into the coordinator
			c.tasks[args.Id] = *replyTask
		}
	} else {
		// dunno
	}
	return nil
}

func (c *Coordinator) addTask(task Task) {

}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func (c *Coordinator) TaskSuccessful(args *WorkerServer, reply *struct{}) error {
	task := c.tasks[args.Id]
	fmt.Printf("TaskSuccessful task %#v\n", task)
	fmt.Printf("TaskSuccessful args %#v\n", args)

	if (task.TaskStatus == Aborted) || (task == Task{}) {
		fmt.Println("WARNING receiving success result of a aborted task")
		return nil
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

	// // todo remove remaingFiles
	// fmt.Println("c.runningFiles", c.runningFiles)
	// c.runningFiles = remove(c.runningFiles, args.TaskFileName)
	// fmt.Println("c.runningFiles", c.runningFiles)

	// return nil
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

	c.runningType = "map"
	c.nReduce = nReduce

	c.tasks = make(map[int]Task, 0)

	// Your code here.

	c.server()
	return &c
}
