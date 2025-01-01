package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	files          []string
	nReduce        int
	mapTasks       map[int]*TaskState
	reduceTasks    map[int]*TaskState
	mapPhase       bool
	done           bool
	mu             sync.Mutex
	inProgressMaps map[int]string
}

type TaskState struct {
	State     string    // "idle", "in-progress", or "completed"
	StartTime time.Time // start time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignWork(args *TaskArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		reply.TaskType = "done"
		return nil
	} else if c.mapPhase {
		for taskNum, task := range c.mapTasks {
			if task.State == "idle" {
				task.State = "in-progress"
				task.StartTime = time.Now()
				reply.TaskType = "map"
				reply.MapNumber = taskNum
				reply.FileName = c.files[taskNum]
				reply.NReduce = c.nReduce
				return nil
			}
		}
	} else {
		for taskNum, task := range c.reduceTasks {
			if task.State == "idle" {
				task.State = "in-progress"
				task.StartTime = time.Now()
				reply.TaskType = "reduce"
				reply.ReduceNumber = taskNum
				reply.NMap = len(c.files)
				return nil
			}
		}
	}

	reply.TaskType = "wait"
	return nil
}

func (c *Coordinator) SaveReduceOutput(args *ReduceOutputArgs, _ *struct{}) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    filename := fmt.Sprintf("mr-out-%d.json", args.ReduceNumber)
    file, err := os.Create(filename)
    if err != nil {
        log.Printf("Failed to create output file %v: %v", filename, err)
        return err
    }
    defer file.Close()

    for key, value := range args.Output {
        fmt.Fprintf(file, "%v %v\n", key, value)
    }

    return nil
}

func (c *Coordinator) ReportCompletion(args *ReportCompletionArgs, _ *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		c.mapTasks[args.MapNumber].State = "completed"
		if c.allTasksCompleted(c.mapTasks) {
			c.mapPhase = false
		}
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.ReduceNumber].State = "completed"
		if c.allTasksCompleted(c.reduceTasks) {
			c.done = true
		}
	}
	return nil
}

func (c *Coordinator) ReportFailure(args *ReportFailureArgs, _ *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		// Mark the map task as idle again to retry
		c.mapTasks[args.MapNumber].State = "idle"
	} else if args.TaskType == "reduce" {
		// Mark the reduce task as idle again to retry
		c.reduceTasks[args.ReduceNumber].State = "idle"
	}

	return nil
}

func (c *Coordinator) allTasksCompleted(tasks map[int]*TaskState) bool {
	for _, task := range tasks {
		if task.State != "completed" {
			return false
		}
	}
	return true
}

func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		if c.done {
			c.mu.Unlock()
			return
		}

		now := time.Now()
		for _, task := range c.mapTasks {
			if task.State == "in-progress" && now.Sub(task.StartTime) > 10*time.Second {
				task.State = "idle"
			}
		}
		for _, task := range c.reduceTasks {
			if task.State == "in-progress" && now.Sub(task.StartTime) > 10*time.Second {
				task.State = "idle"
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make(map[int]*TaskState),
		reduceTasks: make(map[int]*TaskState),
		mapPhase:    true,
		done:        false,
	}

	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = &TaskState{State: "idle"}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskState{State: "idle"}
	}

	go c.monitorTasks()
	c.server()
	return &c
}
