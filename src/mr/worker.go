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

	for {
		task, err := requestTask()
		if err != nil {
			fmt.Println(err)
		}

		if task.Name == exitTask {
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

// mapSplit applies map function according to Task instructions
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

// reduceSplits reduces n splits assigned as per filesuffix
func reduceSplits(task Task, reducef func(string, []string) string) error {
	filesuffix := task.FileName
	filePaths, err := filepath.Glob("*_" + filesuffix + ".txt")
	if err != nil {
		return err
	}
	var keyValue KeyValue
	var intermediate []KeyValue

	for _, filePath := range filePaths {

		file, err := os.Open(filePath)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(file)

		for {
			err := decoder.Decode(&keyValue)
			if err != nil && err == io.EOF {
				break
			}

			if err != nil {
				return err
			}
			intermediate = append(intermediate, keyValue)
		}

		file.Close()
	}
	ofile := fmt.Sprintf("mr-out-%s", task.FileName)
	file, err := ioutil.TempFile("", ofile+"*")
	if err != nil {
		return err
	}
	sort.Sort(ByKey(intermediate))

	//we have slice of key values or intermediate here after that we will use code from mrsequential.go
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	//atomically rename file to avoid conflicts between concurrent workers
	err = os.Rename(file.Name(), ofile)
	if err != nil {
		return err
	}
	file.Close()

	// inform the coordinator after completing the task
	call(completeTaskFunction, &CompleteArgs{Task: task}, &CompleteReply{})
	return nil
}

// requestTask takes next task from coordinator
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

// writeToBuckets assigns mapped results to files according to ihash
func writeToBuckets(data []KeyValue, task Task) error {
	sort.Sort(ByKey(data))

	var output KeyValue

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

		// hash  and assign new writer to encoder only if the current key != prevKey
		if key != prevKey {
			hash := ihash(key)
			bucketIndex = hash % task.Buckets
			bucket := buckets[bucketIndex]
			enc = json.NewEncoder(bucket)
		}
		output.Key = kv.Key
		output.Value = kv.Value
		enc.Encode(output)
		prevKey = key

	}
	for i, tempFile := range buckets {
		tempFile.Close()
		err := os.Rename(tempFile.Name(), fmt.Sprintf("mr_%d_%d.txt", task.WorkerNum, i))
		if err != nil {
			return err
		}

	}

	// call the coordinator to make the task completed
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
