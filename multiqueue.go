package multiqueue

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultConcurrency = 10
)

// MultiQ data structure
type MultiQ struct {
	// Concurrency level
	Concurrency int
	// Done use when the user is done
	Done chan bool
	// Resume poke the pool to go through the queues again
	Resume chan bool

	logger *log.Logger
	wg     sync.WaitGroup
	queues []*Queue
}

// Task data structure
type Task struct {
	Name  string
	Run   func()
	queue *Queue
}

// New Constructor
func New(opts ...Option) *MultiQ {
	mq := &MultiQ{
		Concurrency: defaultConcurrency,
		logger:      log.New(os.Stderr, fmt.Sprintf("[%v] ", os.Getpid()), log.Ldate|log.Lmicroseconds),
		Done:        make(chan bool),
		Resume:      make(chan bool),
	}

	for _, opt := range opts {
		opt(mq)
	}

	return mq
}

// Option MultiQ options type
type Option func(*MultiQ)

// WithConcurrency Sets the concurrency
func WithConcurrency(c int) Option {
	return func(mq *MultiQ) {
		mq.Concurrency = c
	}
}

// WithLogger Sets the logger
func WithLogger(l *log.Logger) Option {
	return func(mq *MultiQ) {
		mq.logger = l
	}
}

// AddQueue Adds a queue
func (mq *MultiQ) AddQueue(nq *Queue) error {
	for _, q := range mq.queues {
		if strings.Compare(nq.Name, q.Name) == 0 {
			return fmt.Errorf("queue with name [%s] already exists", nq.Name)
		}
	}
	mq.queues = append(mq.queues, nq)
	return nil
}

// Run Start processing the queue
func (mq *MultiQ) Run() {
	mq.logger.Printf("MultiQueue starts: %v\n", mq)
	mq.wg.Add(mq.Concurrency)

	mq.pool()

	mq.wg.Wait()
	mq.dbug("Run is done")
}

func (mq *MultiQ) pool() {
	tasksCh := make(chan *Task, 100)

	for i := 0; i < mq.Concurrency; i++ {
		mq.dbug("+ spine up worker %v", i+1)
		go mq.worker(i, tasksCh)
	}

	for {
		mq.dbug("--- start checking queues ---")
		allEmpty := true
		for _, q := range mq.queues {

			allEmpty = allEmpty && q.IsEmpty()
			if allEmpty {
				mq.dbug("all empty after checking q [%s]: %v", q.Name, allEmpty)
				continue
			}

			task, ok := q.Dequeue()
			if !ok {
				continue
			}
			mq.dbug("<- task %v dequeued", task.Name)

			tasksCh <- task

		}

		if allEmpty {
			mq.logger.Printf("All queues are empty, running idle ...")

			select {
			case <-mq.Done:
				mq.dbug("===> received Done")
				close(tasksCh)
				mq.dbug("XX task channel closed")
				return
			case <-mq.Resume:
				mq.dbug("===> received Resume")
				d := time.Duration(1 * time.Second)
				time.Sleep(d)
				continue
			}

		} else {
			time.Sleep(time.Duration(time.Duration(500) * time.Millisecond))
		}
	}
}

func (mq *MultiQ) worker(id int, in <-chan *Task) {
	defer mq.wg.Done()
	for {
		task, ok := <-in
		if !ok {
			return
		}

		mq.dbug("-> worker %d: running task %s", id, task.Name)
		task.Run()
		task.queue.DecRun()

		mq.dbug("worker %d: task %v is done", id, task.Name)
	}
}

func (mq *MultiQ) dbug(format string, args ...interface{}) {
	format = "DBUG - " + format
	mq.logger.Printf(format, args...)
}
