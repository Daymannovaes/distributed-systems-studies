package mr

import (
	"hash/fnv"
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
}

type Task struct {
	Filename string

	TaskType string
	NReduce  int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (j *Task) mapHashNumber(value string) int {
	return ihash(value) % j.NReduce
}
