package mr

import (
	"fmt"
	"testing"
	"time"
)

func TestQueueIdle(t *testing.T) {
	q := NewQueue(10)

	select {
	case <-q.Idle():
	default:
		t.Errorf("NewQueue(10) should not be idle")
	}
	for i := 0; i < 10; i++ {

		q.Add(func() {
			fmt.Println("the job started")

		})
	}

	fmt.Println("All jobs started")
	/*for i := 0; i < 9; i++ {
		go func() {
			q.CompleteTask()
		}()

	}
	*/
	q.Add(func() {
		fmt.Println("The job started")
		time.Sleep(5 * time.Second)

	})

	<-q.Done()
	fmt.Println("All the jobs exited")

}
