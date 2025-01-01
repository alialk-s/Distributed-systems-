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
			log.Printf("Retrying to connect to the coordinator...")
        		time.Sleep(2 * time.Second)
        		continue
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

    output := make(map[string]string)
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
        output[intermediate[i].Key] = reducef(intermediate[i].Key, values)
        i = j
    }

    call("Coordinator.SaveReduceOutput", &ReduceOutputArgs{
        ReduceNumber: task.ReduceNumber,
        Output:       output,
    }, nil)

    call("Coordinator.ReportCompletion", &ReportCompletionArgs{
        TaskType:     "reduce",
        ReduceNumber: task.ReduceNumber,
        MapNumber:    -1,
    }, nil)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "10.214.0.2"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
