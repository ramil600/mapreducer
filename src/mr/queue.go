package mr

import (
	"fmt"
	"sync"
)

type Queue struct {
	tmu       sync.Mutex
	tasksToDo []Task

	st chan queueState
}

type queueState struct {
	active    int // number of active workers on given tasks
	started   int
	completed int
	//backlog   []Task
	idle chan struct{} // if non-nil, closed when active becomes 0
	done chan struct{}
}

func NewQueue(tasksToDo []Task) *Queue {

	q := &Queue{
		tasksToDo: tasksToDo,
		st:        make(chan queueState, 1),
	}
	q.tmu.Lock()
	q.st <- queueState{
		started: len(tasksToDo),
	}
	q.tmu.Unlock()

	return q

}

func (q *Queue) Done() <-chan struct{} {
	st := <-q.st
	defer func() { q.st <- st }()

	if st.done == nil {
		st.done = make(chan struct{})
		if st.active == 0 && st.completed == st.started {
			close(st.done)
		}

	}

	return st.done

}

func (q *Queue) CompleteTask() {
	q.tmu.Lock()
	st := <-q.st
	st.completed++
	if st.done != nil && st.completed == st.started {
		close(st.done)
	}
	q.st <- st
	q.tmu.Unlock()
}

func (q *Queue) Add(f func()) {
	st := <-q.st

	if st.active == 0 {
		st.idle = nil
	}
	st.active++

	q.st <- st
	go func() {
		f()

		st := <-q.st
		if st.active--; st.active == 0 && st.idle != nil {
			close(st.idle)
		}
		q.st <- st

	}()
}

func (q *Queue) Idle() <-chan struct{} {
	st := <-q.st
	defer func() { q.st <- st }()

	if st.idle == nil {
		st.idle = make(chan struct{})
		if st.active == 0 {
			close(st.idle)
		}
	}

	return st.idle

}

func (q *Queue) ReturnNextTask() (Task, error) {
	q.tmu.Lock()
	defer q.tmu.Unlock()
	if len(q.tasksToDo) == 0 {

		return Task{Name: exitTask}, fmt.Errorf("Queue is idle")
	}
	task := q.tasksToDo[0]
	if len(q.tasksToDo) > 1 {
		q.tasksToDo = q.tasksToDo[1:]
	} else {
		q.tasksToDo = []Task{}
	}

	q.Add(func() {})

	return task, nil

}
