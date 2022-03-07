package mr

import "log"
import "net"
import "fmt"
import "os"
import "sync"
import "net/rpc"
import "net/http"


type Coordinator struct {
	WorkerNum int
	WorkerState []int       // -1 - dead, 0 - idle, 1 - working
	WorkerCallTime []int
	MapTaskState []int      // 0 - no allocate, 1 - dealing, 2 - completed
	ReduceTaskState []int   // 0 - no allocate, 1 - dealing, 2 - completed
	Filename []string
	Map2Worker []int
	nReduce int
	Reduce2Worker []int
	mux sync.Mutex
}

func (c *Coordinator) GetID(args *GetIdArgs, reply *GetIdReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.ID = c.WorkerNum
	c.WorkerNum++
	c.WorkerState = append(c.WorkerState, 0)
	c.WorkerCallTime = append(c.WorkerCallTime, 0)
	return nil
}

func (c *Coordinator) DealFailed(args *DealFailedArgs, reply *DealFailedReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.WorkerState[args.ID] = 0
	c.WorkerCallTime[args.ID] = 0
	c.reallocate(args.ID)
	return nil
}

func (c *Coordinator) PrintState() {
	doneMapNum := 0
	doneReduceNum := 0
	for _, state := range c.MapTaskState {
		if state == 2 {
			doneMapNum++
		}
	}
	for _, state := range c.ReduceTaskState {
		if state == 2 {
			doneReduceNum++
		}
	}
	fmt.Printf("%d/%d map tasks done, %d/%d reduce tasks done!\n",
			   doneMapNum, len(c.MapTaskState), doneReduceNum, len(c.ReduceTaskState))
}

func (c *Coordinator) MapSuccess(args *MapSuccessArgs, reply *MapSuccessReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.WorkerState[args.ID] = 0
	c.WorkerCallTime[args.ID] = 0
	c.MapTaskState[args.MapTaskID] = 2
	c.Map2Worker[args.MapTaskID] = -2
	c.PrintState()
	return nil
}

func (c *Coordinator) ReduceSuccess(args *ReduceSuccessArgs, reply *ReduceSuccessReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.WorkerState[args.ID] = 0
	c.WorkerCallTime[args.ID] = 0
	c.ReduceTaskState[args.ReduceTaskID] = 2
	c.Reduce2Worker[args.ReduceTaskID] = -2
	c.PrintState()
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// map task
	c.mux.Lock()
	defer c.mux.Unlock()
	allMapCompleted := true
	for i := range c.MapTaskState {
		if c.MapTaskState[i] == 0 {
			reply.TaskType = 1
			reply.MapTaskID = i
			reply.MapFilename = c.Filename[i]
			reply.ReduceNum = c.nReduce
			c.WorkerState[args.ID] = 1
			c.WorkerCallTime[args.ID] = 0
			c.MapTaskState[i] = 1
			c.Map2Worker[i] = args.ID
			return nil
		} else if c.MapTaskState[i] != 2 {
			allMapCompleted = false
		}
	}
	// reduce task
	if allMapCompleted == true {
		for i := range c.ReduceTaskState {
			if c.ReduceTaskState[i] == 0 {
				reply.TaskType = 2
				reply.ReduceTaskID = i
				reply.MapNum = len(c.MapTaskState)
				c.WorkerState[args.ID] = 1
				c.WorkerCallTime[args.ID] = 0
				c.ReduceTaskState[i] = 1
				c.Reduce2Worker[i] = args.ID
				return nil
			}
		}
	}
	reply.TaskType = 0
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) reallocate(ID int) {
	for i := range c.Map2Worker {
		if c.Map2Worker[i] == ID {
			c.MapTaskState[i] = 0
			c.Map2Worker[i] = -1
			return
		}
	}
	for i := range c.Reduce2Worker {
		if c.Reduce2Worker[i] == ID {
			c.ReduceTaskState[i] = 0
			c.Reduce2Worker[i] = -1
			return
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	for i := range c.WorkerCallTime {
		c.WorkerCallTime[i]++
		if c.WorkerCallTime[i] >= 10 {
			c.WorkerState[i] = -1
			c.reallocate(i)
		}
	}
	ret := true
	for i := range c.MapTaskState {
		if c.MapTaskState[i] != 2 {
			ret = false
			break
		}
	}
	if ret == true {
		for i := range c.ReduceTaskState {
			if c.ReduceTaskState[i] != 2 {
				ret = false
				break
			}
		}
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.WorkerNum = 0
	c.Filename = files
	c.nReduce = nReduce
	for i := 0; i < len(c.Filename); i++ {
		c.MapTaskState = append(c.MapTaskState, 0)
		c.Map2Worker = append(c.Map2Worker, -1)
	}
	for i := 0; i < c.nReduce; i++ {
		c.ReduceTaskState = append(c.ReduceTaskState, 0)
		c.Reduce2Worker = append(c.Reduce2Worker, -1)
	}
	c.server()
	return &c
}
