package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type GetIdArgs struct {
}

type GetIdReply struct {
	ID int
}

type GetTaskArgs struct {
	ID int
}

type GetTaskReply struct {
	TaskType int   // -1 - exit, 0 - idle, 1 - map task, 2 - reduce task
	MapTaskID int
	MapFilename string
	ReduceTaskID int
	ReduceNum int
	MapNum int
}

type DealFailedArgs struct {
	ID int
}

type DealFailedReply struct {
}

type MapSuccessArgs struct {
	ID int
	MapTaskID int
}

type MapSuccessReply struct {
}

type ReduceSuccessArgs struct {
	ID int
	ReduceTaskID int
}

type ReduceSuccessReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
