package multiqueue

import (
	"container/list"
	"fmt"
	"log"
	"os"
)

const unlimited int = -1

// Queue is the queue which uses by MultiQ.
type Queue struct {
	// Name of the queue
	Name string
	// Concurrency level. Default: -1 (unlimited)
	Concurrency int
	// Debug level, the higher the noisier. Default: 0
	Debug int

	tasks   *list.List
	running int
	logger  *log.Logger
}

// QOption Queue option.
type QOption func(*Queue)

// NewQ create a new queue.
func NewQ(name string, opts ...QOption) *Queue {
	q := &Queue{
		Name:        name,
		Concurrency: unlimited,
		Debug:       0,

		tasks:   list.New(),
		running: 0,
		logger:  log.New(os.Stderr, fmt.Sprintf("[%v] ", os.Getpid()), log.Ldate|log.Lmicroseconds),
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

// WithQueueConcurrency set concurrency level of Queue.
func WithQueueConcurrency(c int) QOption {
	return func(q *Queue) {
		q.Concurrency = c
	}
}

// WithQueueLogger set the logger.
func WithQueueLogger(l *log.Logger) QOption {
	return func(q *Queue) {
		q.logger = l
	}
}

// WithQueueDebug set the debug level of Queue.
func WithQueueDebug(d int) QOption {
	return func(q *Queue) {
		q.Debug = d
	}
}

// Enqueue a task.
func (q *Queue) Enqueue(t *Task) {
	q.tasks.PushBack(t)
	t.queue = q

	q.dbug("enqueued task [%s] -> [%v]: length: %v", t.Name, q.Name, q.tasks.Len())
}

// Dequeue a task from queue. If the concurrency set, it only dequeue if there are no more than concurrency level jobs running. Returns false either the queue is empty or the concurrency hit the limit.
func (q *Queue) Dequeue() (*Task, bool) {

	if q.Concurrency > 0 && q.running >= q.Concurrency {
		q.dbug2("hit the limit of concurrency [%v] for queue [%s]", q.Concurrency, q.Name)
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

// IncRun increment the number of running jobs.
func (q *Queue) IncRun() {
	q.running++
}

// DecRun decrement the number of running jobs.
func (q *Queue) DecRun() {
	q.running--
}

func (q *Queue) dbug(format string, args ...interface{}) {
	if q.Debug < 1 {
		return
	}
	format = "DBUG - " + format
	q.logger.Printf(format, args...)
}

func (q *Queue) dbug2(format string, args ...interface{}) {
	if q.Debug < 2 {
		return
	}
	q.dbug(format, args...)
}
