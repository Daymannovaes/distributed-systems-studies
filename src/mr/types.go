package mr

import (
	"hash/fnv"
	"strconv"
	"time"
)

// ---- WORKER Types

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

/*
Example of MapFnType return:
[

	{key: 'a', value: 1},
	{key: 'a', value: 1},
	{key: 'b', value: 1},
	{key: 'c', value: 1},

]
*/
type MapFnType func(string, string) []KeyValue
type ReduceFnType func(string, []string) string

type WorkerServer struct {
	Id int

	NReduce int

	mapf    MapFnType
	reducef ReduceFnType

	// CurrentTask Task
	currentTask Task
}

// task is returned by the coordinator and will be kept in the workerServer abstraction

type TaskType int
type TaskStatus int

const (
	Map TaskType = iota
	Reduce
	Empty

	Shutdown // with this type, coordinator can tell the worker to shutdown itself
)

const (
	Started TaskStatus = iota
	Finished
	Aborted
)

type Task struct {
	Filename   string
	TaskType   TaskType
	TaskStatus TaskStatus
	StartedAt  time.Time
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *WorkerServer) mapHashNumber(value string) int {
	return ihash(value) % w.NReduce
}

// return mr-x-y where x is id of the worker and y is the reduce task number
func (w *WorkerServer) mapHashfile(value string) string {
	println("w.Id: ", w.Id)
	println("value: ", value)

	return "mr-" + strconv.Itoa(w.Id) + "-" + strconv.Itoa(w.mapHashNumber(value))
}
