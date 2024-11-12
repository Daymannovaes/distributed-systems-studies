package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"
)

var workerServer WorkerServer

// main/mrworker.go calls this function.
func Worker(
	mapf MapFnType,
	reducef ReduceFnType,
) {
	workerServer = WorkerServer{mapf: mapf, reducef: reducef}
	registerWorker()

	println("worker registered successfully: ", workerServer.Id)

	startListeningForTasks()
}

func startListeningForTasks() {
	task := askForATask()
	workerServer.currentTask = task

	for task.TaskType != Shutdown {
		fmt.Println("workerServer: ")
		fmt.Printf("%#v\n", workerServer)

		if task.TaskType != Empty {
			executeTask(task)

			callTaskSuccessful()
		}

		task = askForATask()

		time.Sleep(time.Second)
	}
}

func executeTask(task Task) {
	// fake timer just to simulate a hang
	// if workerServer.Id == 1 {
	// 	time.Sleep(time.Second * 20)
	// }

	if task.TaskType == Map {
		contentBites, error := os.ReadFile(task.Filename)

		if error != nil {
			log.Fatal("dialing:", error)
		}

		content := string(contentBites)

		// print("content is: ", content)
		mapResult := workerServer.mapf(task.Filename, content)

		// var intermediate [][]string = make([][]string, workerServer.NReduce)

		var intermediateArray map[string][]KeyValue = make(map[string][]KeyValue)
		var intermediateMap map[string]map[string][]string = make(map[string]map[string][]string)
		for _, keyValue := range mapResult {
			filename := workerServer.mapHashfile(keyValue.Key)

			if intermediateArray[filename] == nil {
				intermediateArray[filename] = []KeyValue{}
			}

			intermediateArray[filename] = append(intermediateArray[filename], keyValue)

			if intermediateMap[filename] == nil {
				intermediateMap[filename] = make(map[string][]string)
			}
			if intermediateMap[filename][keyValue.Key] == nil {
				intermediateMap[filename][keyValue.Key] = []string{}
			}

			intermediateMap[filename][keyValue.Key] = append(intermediateMap[filename][keyValue.Key], keyValue.Value)
		}

		// fmt.Println("a3: ", intermediateMap)

		// fmt.Println("Intermediate: ", intermediate)
	}
}

// ----- RPC Calls
func registerWorker() {
	ok := call("Coordinator.RegisterWorker", &struct{}{}, &workerServer)

	if ok {
		println("worker registered ", workerServer.Id)
	} else {
		println("error registering worker")
	}

	// return reply
}

func askForATask() Task {
	reply := Task{}
	ok := call("Coordinator.AskForATask", &workerServer, &reply)

	if ok {
		println("file is ", reply.Filename)
		println("job type is ", reply.TaskType)
	} else {
		println("error")
	}

	return reply
}

func callTaskSuccessful() {
	// se é task succesfull pq to passando worker? tinha que ser uma mistura dos 2
	ok := call("Coordinator.TaskSuccessful", &workerServer, &struct{}{})

	if ok {
		println("callTaskSuccessful")
	} else {
		println("error callTaskSuccessful")
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
