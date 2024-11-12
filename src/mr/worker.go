package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

// main/mrworker.go calls this function.
func Worker(
	mapf MapFnType,
	reducef ReduceFnType,
) {
	workerServer := registerWorker()

	println("worker registered successfully: ", workerServer.Id)

	task := askForATask()

	if task.TaskType == "map" {
		contentBites, error := os.ReadFile(task.Filename)

		if error != nil {
			log.Fatal("dialing:", error)
		}

		content := string(contentBites)

		// print("content is: ", content)
		mapResult := mapf(task.Filename, content)

		var intermediate [][]string = make([][]string, task.NReduce)
		for _, value := range mapResult {
			// println("key: ", value.Key, " | value: ", value.Value)
			hashValue := task.mapHashNumber(value.Key)
			if intermediate[hashValue] == nil {
				intermediate[hashValue] = []string{}
			}

			intermediate[hashValue] = append(intermediate[hashValue], value.Value)
		}

		// fmt.Println("Intermediate: ", intermediate)
	}
}

// ----- RPC Calls
func registerWorker() WorkerServer {
	reply := WorkerServer{}

	ok := call("Coordinator.RegisterWorker", &struct{}{}, &reply)

	if ok {
		print("worker registered ", reply.Id)
	} else {
		println("error registering worker")
	}

	return reply
}

func askForATask() Task {
	reply := Task{}
	ok := call("Coordinator.AskForAJob", &struct{}{}, &reply)

	if ok {
		print("file is ", reply.Filename)
		print("job type is ", reply.TaskType)
	} else {
		println("error")
	}

	return reply
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
