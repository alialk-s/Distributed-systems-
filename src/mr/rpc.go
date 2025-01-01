package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type ExampleReply struct {
	TaskType     string
	MapNumber    int
	ReduceNumber int
	FileName     string
	NReduce      int
	NMap         int
}

type ReportFailureArgs struct {
	TaskType     string
	MapNumber    int
	ReduceNumber int
	ErrorMsg     string // to send failure details to the coordinator.
}

type ReduceOutputArgs struct {
    ReduceNumber int
    Output       map[string]string
}

// Add your RPC definitions here.
type TaskArgs struct{}

type ReportCompletionArgs struct {
	TaskType     string
	MapNumber    int
	ReduceNumber int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
