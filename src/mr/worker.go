package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

	for {
		task := ExampleReply{}
		ok := call("Coordinator.AssignWork", &TaskArgs{}, &task)
		if !ok {
			log.Fatalf("Worker failed to connect to coordinator")
		}

		switch task.TaskType {
		case "map":
			executeMapTask(task, mapf)
		case "reduce":
			executeReduceTask(task, reducef)
		case "wait":
			// No tasks available, wait before retrying.
			continue
		case "done":
			return
		}
	}
}

func executeMapTask(task ExampleReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Printf("Cannot open %v. Reporting failure.", task.FileName)
		reportFailureToCoordinator(task, "map", err.Error())
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("Cannot read %v. Reporting failure.", task.FileName)
		reportFailureToCoordinator(task, "map", err.Error())
		return
	}
	file.Close()

	intermediate := mapf(task.FileName, string(content))
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d.json", task.MapNumber, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Cannot encode %v", kv)
			}
		}
		file.Close()

	}

	call("Coordinator.ReportCompletion", &ReportCompletionArgs{
		TaskType:     "map",
		MapNumber:    task.MapNumber,
		ReduceNumber: -1,
	}, nil)
}

func executeReduceTask(task ExampleReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d.json", i, task.ReduceNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Cannot open %v. Reporting failure.", filename)
			reportFailureToCoordinator(task, "reduce", err.Error())
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	outputFile := fmt.Sprintf("mr-out-%d.json", task.ReduceNumber)
	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Cannot create %v", outputFile)
	}
	defer out.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(out, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	call("Coordinator.ReportCompletion", &ReportCompletionArgs{
		TaskType:     "reduce",
		MapNumber:    -1,
		ReduceNumber: task.ReduceNumber,
	}, nil)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
/*

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}*/

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

func reportFailureToCoordinator(task ExampleReply, taskType, errorMsg string) {
	call("Coordinator.ReportFailure", &ReportFailureArgs{
		TaskType:     taskType,
		MapNumber:    task.MapNumber,
		ReduceNumber: task.ReduceNumber,
		ErrorMsg:     errorMsg,
	}, nil)
}
