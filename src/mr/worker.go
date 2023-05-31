package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
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

	// Your worker implementation here.
	for {
		// uncomment to send the Example RPC to the coordinator.
		//CallExample()
		task, err := requestTask()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Task received: ", task.Name, task.FileName)

		if task.Name == exitTask {
			fmt.Println("exiting on request")
			return
		}
		if task.Name == mapTask {
			mapSplit(*task, mapf)
		}
		if task.Name == reduceTask {
			err := reduceSplits(*task, reducef)
			if err != nil {
				fmt.Println(err)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

}

// example function to show how to make an RPC call to the coordinator.
//

func mapSplit(task Task, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	writeToBuckets(intermediate, task)

}
func reduceSplits(task Task, reducef func(string, []string) string) error {
	filesuffix := task.FileName
	filePaths, err := filepath.Glob("*_" + filesuffix + ".txt")
	if err != nil {
		return err
	}
	reducedData := make(map[string]int)
	for _, filePath := range filePaths {

		file, err := os.Open(filePath)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(file)

		var keyValue struct {
			Key   string
			Value int
		}
		var keyValues []struct {
			Key   string
			Value int
		}
		for {
			err := decoder.Decode(&keyValue)
			if err != nil && err == io.EOF {
				break
			}

			if err != nil {
				return err
			}
			keyValues = append(keyValues, keyValue)
		}

		for _, kv := range keyValues {
			reducedData[kv.Key] += kv.Value
		}
		file.Close()
	}
	ofile := fmt.Sprintf("mr-out-%s", task.FileName)
	//file, err := os.Create(ofile)
	file, err := ioutil.TempFile("", ofile+"*")
	if err != nil {
		return err
	}

	for key, value := range reducedData {
		fmt.Fprintf(file, "%v %v\n", key, value)
	}
	err = os.Rename(file.Name(), ofile)
	if err != nil {
		return err
	}
	file.Close()

	call(completeTaskFunction, &CompleteArgs{Task: task}, &CompleteReply{})

	return nil
}

func requestTask() (*Task, error) {
	args := MapArgs{}
	reply := MapReply{}
	ok := call(requestTaskFunction, &args, &reply)
	if !ok {
		return nil, fmt.Errorf("couldnt request a new task from server")
	}
	task := reply.Task
	return &task, nil
}

func writeToBuckets(data []KeyValue, task Task) error {
	sort.Sort(ByKey(data))

	var outputkv struct {
		Key   string
		Value int
	}
	buckets := make([]*os.File, task.Buckets)

	for i := 0; i < task.Buckets; i++ {
		fileName := fmt.Sprintf("mr_%d_%d.txt", task.WorkerNum, i)
		file, err := ioutil.TempFile("", fileName+"*")
		if err != nil {
			return err
		}
		buckets[i] = file
	}

	var prevKey string
	var bucketIndex int
	var enc *json.Encoder
	for _, kv := range data {
		key := kv.Key
		value, _ := strconv.Atoi(kv.Value)

		// hash  and assign new writer to encoder only if the current key != prevKey
		if key != prevKey {
			hash := ihash(key)
			bucketIndex = hash % task.Buckets
			bucket := buckets[bucketIndex]
			enc = json.NewEncoder(bucket)
		}
		outputkv.Key = key
		outputkv.Value = value
		enc.Encode(outputkv)
		prevKey = key

	}
	for i, tempFile := range buckets {
		tempFile.Close()
		err := os.Rename(tempFile.Name(), fmt.Sprintf("mr_%d_%d.txt", task.WorkerNum, i))
		if err != nil {
			return err
		}

	}

	call(completeTaskFunction, &CompleteArgs{Task: task}, &CompleteReply{})

	return nil
}

// the RPC argument and reply types are defined in rpc.go.
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
