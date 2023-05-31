package mr

import (
	"context"
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
	jobs            *Queue
	jobsch          chan Task         // queue of jobs distributed concurrently
	buckets         int               // total number for buckets for reducers
	tasksToComplete map[Task]struct{} // map to verift tasks that are due to be completed
	inReduce        bool              // to check if reduce stage started
	sync.Mutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

var (
	requestTaskFunction  = "Coordinator.MapArgs"
	completeTaskFunction = "Coordinator.CompleteArgs"
)

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) MapArgs(args *MapArgs, reply *MapReply) error {

	if c.Done() {
		fmt.Println("All the tasks are executed")
		reply.Task.Name = exitTask
		c.cancel()
		return nil
	}

	task := c.returnNextTask()

	reply.Task = task
	//c.Queue Pop Task

	return nil

}

func (c *Coordinator) CompleteArgs(args *CompleteArgs, reply *CompleteReply) error {
	task := args.Task
	c.jobs.CompleteTask()
	c.Lock()
	delete(c.tasksToComplete, task)
	c.Unlock()
	c.wg.Done()
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
		c.cancel()
		fmt.Println("Have we reached here?")
		return true
	}

	return false
}

// Your code here.

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	c := Coordinator{
		buckets:         nReduce,
		tasksToComplete: make(map[Task]struct{}),
		cancel:          cancel,
		jobsch:          make(chan Task, 10),
	}
	tasks := make([]Task, 0)

	for i, file := range files {
		task := Task{
			Name:      mapTask,
			FileName:  file,
			Buckets:   nReduce,
			WorkerNum: i,
		}
		c.tasksToComplete[task] = struct{}{}
		tasks = append(tasks, task)
		c.jobsch <- task
		c.wg.Add(1)
	}
	c.jobs = NewQueue(tasks)

	c.server()
	go c.reconsile(ctx)
	return &c
}

func (c *Coordinator) makeReducerTasks() {
	c.wg.Wait()
	c.Lock()
	c.tasksToComplete = make(map[Task]struct{})
	tasks := make([]Task, 0)
	for i := 0; i < c.buckets; i++ {
		task := Task{
			Name:      reduceTask,
			FileName:  fmt.Sprintf("%d", i),
			Buckets:   c.buckets,
			WorkerNum: i,
		}
		c.tasksToComplete[task] = struct{}{}
		c.wg.Add(1)
		tasks = append(tasks, task)
	}
	c.jobs = NewQueue(tasks)
	c.inReduce = true
	c.Unlock()
	go func() {
		c.wg.Wait()
		close(c.jobsch)
	}()

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
	if lenTasks == 0 {
		c.makeReducerTasks()
	}
	c.Lock()
	task, err := c.jobs.ReturnNextTask()
	c.Unlock()
	if err != nil {
		fmt.Println(err)
	}
	select {
	case task, ok := <-c.jobsch:
		if !ok {
			return Task{Name: exitTask}
		}
		return task
	default:
	}
	return task
}

func (c *Coordinator) reconsile(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("exiting reconsile goroutine")
			return
		case <-time.After(10 * time.Second):
			var tasks []Task
			c.Lock()

			tasksNum := len(c.tasksToComplete)
			if tasksNum == 0 {
				fmt.Println("No new tasks in the queue")
			}
			for task := range c.tasksToComplete {
				tasks = append(tasks, task)

			}
			if len(tasks) != 0 {
				c.jobs = NewQueue(tasks)
			}

			c.Unlock()

		}
	}

}

/*
func (c *Coordinator) listenForReplies() {

}
*/
