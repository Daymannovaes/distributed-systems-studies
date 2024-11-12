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
		mapResult := workerServer.mapf(task.Filename, content)

		var intermediateArray map[string][]KeyValue = make(map[string][]KeyValue)

		/*
		   (
		     'filename-1' => (
		       'wordA' => [1, 1, 1, 1],
		       'wordB' => [1, 1, 1, 1],
		     )
		   )
		*/
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

		// aqui parece melhor usar o intermediateArray, pra ter so 2 aninhamentos ao inves 3
		// porque parece que nao tem muita vantagem agrupar as leituras a nivel de map intermediate file
		// só se eu fosse usar alguma tecnica um pouco mais avançada de particionamento de arquivo
		for filename, keyValues := range intermediateMap {
			fmt.Println("writing to ", filename)
			file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Fatal("error opening file", err)
			}
			defer file.Close()

			for key, values := range keyValues {
				for _, value := range values {
					_, err := file.WriteString(key + " " + value + "\n")
					if err != nil {
						log.Fatal("error writing to file ", err)
					}
				}
			}
		}
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
