package multiqueue

import (
	"container/list"
	"log"
)

// Queue data structure
type Queue struct {
	Name  string
	tasks *list.List
}

// NewQ create a new queue
func NewQ(name string) *Queue {
	return &Queue{
		Name:  name,
		tasks: list.New(),
	}
}

// Enqueue add task to queue
func (q *Queue) Enqueue(t *Task) {
	q.tasks.PushBack(t)
	log.Printf("enqueued task [%s] -> [%v]: length: %v", t.Name, q.Name, q.tasks.Len())
}

// Dequeue remove the task from queue
func (q *Queue) Dequeue() (*Task, bool) {
	t := q.tasks.Front()
	if t == nil {
		return nil, false
	}

	return q.tasks.Remove(t).(*Task), true
}
