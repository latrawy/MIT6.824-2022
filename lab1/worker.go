package mr

import "os"
import "time"
import "fmt"
import "log"
import "sort"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % nReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	ID := -1
	ID = GetID()
	if ID == -1 {
		fmt.Printf("get worker id failed!")
		return
	}

	state := 0
	for state != -1 {
		state = GetTask(ID, mapf, reducef)
		time.Sleep(time.Second)
	}

}

func DealFailed(ID int) {
	args := DealFailedArgs{}
	args.ID = ID
	reply := DealFailedReply{}
	ok := call("Coordinator.DealFailed", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator connect failed!")
	}
}

func MapSuccess(ID int, MapTaskID int) {
	args := MapSuccessArgs{}
	args.ID = ID
	args.MapTaskID = MapTaskID
	reply := MapSuccessReply{}
	ok := call("Coordinator.MapSuccess", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator connect failed!")
	}
}

func ReduceSuccess(ID int, ReduceTaskID int) {
	args := ReduceSuccessArgs{}
	args.ID = ID
	args.ReduceTaskID = ReduceTaskID
	reply := ReduceSuccessReply{}
	ok := call("Coordinator.ReduceSuccess", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator connect failed!")
	}
}

func DealMap(ID int, reply *GetTaskReply,
	mapf func(string, string) []KeyValue) {
	filename := reply.MapFilename
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("cannot open %v", filename)
		DealFailed(ID)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		DealFailed(ID)
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	nReduceKva := [][]KeyValue{}
	for i := 0; i < reply.ReduceNum; i++ {
		nReduceKva = append(nReduceKva, []KeyValue{})
	}
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reply.ReduceNum
		nReduceKva[reduceNum] = append(nReduceKva[reduceNum], kv)
	}
	for i := 0; i < reply.ReduceNum; i++ {
		iname := fmt.Sprintf("mr-%d-%d", reply.MapTaskID, i)
		ifile, _ := os.Create(iname)
		enc := json.NewEncoder(ifile)
		for _, kv := range nReduceKva[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("cannot save %v", iname)
				DealFailed(ID)
				return
			}
		}
	}
	MapSuccess(ID, reply.MapTaskID)
}

func DealReduce(ID int, reply *GetTaskReply,
	reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < reply.MapNum; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTaskID)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskID)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	ReduceSuccess(ID, reply.ReduceTaskID)
}

func GetTask(ID int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) int {
	args := GetTaskArgs{}
	args.ID = ID
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		switch reply.TaskType {
		case -1:
			fmt.Printf("Coordinator connect failed! Exit worker %d", ID)
			return -1
		case 0:
			return 0
		case 1:
			DealMap(ID, &reply, mapf)
			return 0
		case 2:
			DealReduce(ID, &reply, reducef)
			return 0
		}
	} else {
		return -1
	}
	return 0
}

func GetID() int {
	args := GetIdArgs{}
	reply := GetIdReply{}
	ok := call("Coordinator.GetID", &args, &reply)
	if ok {
		fmt.Printf("worker id: %v\n", reply.ID)
		return reply.ID
	} else {
		return -1
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
