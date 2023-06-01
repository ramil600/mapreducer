package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// -----------------------------------------------
type Coordinator struct {
	jobsch          chan Task              // queue of jobs distributed concurrently
	buckets         int                    // total number for buckets for reducers
	tasksToComplete map[Task]chan struct{} // map to verify tasks due to be completed
	inReduce        bool                   // to check if reduce stage started
	sync.Mutex
	wg sync.WaitGroup // wg to synchronize map and reduce stages
}

var (
	requestTaskFunction  = "Coordinator.MapReduceArgs"
	completeTaskFunction = "Coordinator.CompleteArgs"
)

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) MapReduceArgs(args *MapArgs, reply *MapReply) error {

	if c.Done() {
		reply.Task.Name = exitTask
		return nil
	}
	task := c.returnNextTask()
	reply.Task = task
	return nil
}

// CompleteArgs receives complete task acknowledment from concurrent workers
func (c *Coordinator) CompleteArgs(args *CompleteArgs, reply *CompleteReply) error {
	task := args.Task
	c.Lock()
	ch, ok := c.tasksToComplete[task]
	if ch == nil {
		ch = make(chan struct{})
	}
	close(ch)
	//if someone already deleted the task we dont want to send another done
	if ok {
		c.wg.Done()
		delete(c.tasksToComplete, task)
	}
	c.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	c.Lock()
	tasksToComplete := len(c.tasksToComplete)
	inReduce := c.inReduce
	c.Unlock()

	if tasksToComplete == 0 && inReduce {

		return true
	}

	return false
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		buckets:         nReduce,
		tasksToComplete: make(map[Task]chan struct{}),
		jobsch:          make(chan Task, 10),
	}

	for i, file := range files {
		task := Task{
			Name:      mapTask,
			FileName:  file,
			Buckets:   nReduce,
			WorkerNum: i,
		}
		c.tasksToComplete[task] = make(chan struct{})

		c.wg.Add(1)
		c.jobsch <- task

	}

	c.server()
	go c.makeReducerTasks()
	return &c
}

// populates the jobsch with reduce tasks after waiting on map tasks
func (c *Coordinator) makeReducerTasks() {
	c.wg.Wait()
	c.Lock()
	c.tasksToComplete = make(map[Task]chan struct{})

	for i := 0; i < c.buckets; i++ {
		task := Task{
			Name:      reduceTask,
			FileName:  fmt.Sprintf("%d", i),
			Buckets:   c.buckets,
			WorkerNum: i,
		}
		c.tasksToComplete[task] = make(chan struct{})
		c.jobsch <- task
		c.wg.Add(1)

	}
	c.inReduce = true
	c.Unlock()

	c.wg.Wait()
	close(c.jobsch)

}

func (c *Coordinator) returnNextTask() Task {
	task := Task{}
	c.Lock()
	lenTasks := len(c.tasksToComplete)
	c.Unlock()
	if lenTasks == 0 && c.inReduce {
		task.Name = exitTask
		return task
	}

	select {
	case task, ok := <-c.jobsch:
		if !ok {
			return Task{Name: exitTask}
		}
		c.Lock()
		ch := c.tasksToComplete[task]
		c.Unlock()
		go c.reconcileTask(task, ch)
		return task
	}

}

func (c *Coordinator) reconcileTask(task Task, ch chan struct{}) {
	select {
	case <-time.After(10 * time.Second):
		c.Lock()
		//	After 10 seconds added task back to the queue only if it was not deleted by concurrent worker
		if _, ok := c.tasksToComplete[task]; ok {
			c.jobsch <- task
		}
		c.Unlock()
	case <-ch:
		// If we receive on the channel, it was completed and closed; we can exit
		return
	}
}
