package multiqueue

import (
	"container/list"
	"log"
)

// Queue data structure
type Queue struct {
	// Name of the queue
	Name string
	// Concurrency level
	Concurrency int

	tasks   *list.List
	running int
}

// QOption option type
type QOption func(*Queue)

// NewQ create a new queue
func NewQ(name string, opts ...QOption) *Queue {
	q := &Queue{
		Name:        name,
		Concurrency: 1,
		tasks:       list.New(),
		running:     0,
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

// WithQueueConcurrency option to set concurrency level. Default: 1
func WithQueueConcurrency(c int) QOption {
	return func(q *Queue) {
		q.Concurrency = c
	}
}

// Enqueue add task to queue
func (q *Queue) Enqueue(t *Task) {
	q.tasks.PushBack(t)
	t.queue = q

	log.Printf("enqueued task [%s] -> [%v]: length: %v", t.Name, q.Name, q.tasks.Len())
}

// Dequeue remove the task from queue
func (q *Queue) Dequeue() (*Task, bool) {

	if q.Concurrency > 0 && q.running >= q.Concurrency {
		log.Printf("hit the limit of concurrency [%v] for queue [%s]", q.Concurrency, q.Name)
		return nil, false
	}

	t := q.tasks.Front()
	if t == nil {
		return nil, false
	}

	q.IncRun()

	return q.tasks.Remove(t).(*Task), true
}

// IsEmpty true if the queue is empty
func (q *Queue) IsEmpty() bool {
	return q.tasks.Len() == 0
}

// IncRun increment running
func (q *Queue) IncRun() {
	q.running++
}

// DecRun decrement running
func (q *Queue) DecRun() {
	q.running--
}
