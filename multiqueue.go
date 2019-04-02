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
	Concurrency  int
	logger       *log.Logger
	wg           sync.WaitGroup
	queues       []*Queue
	qConcurUsage map[string]int
}

// Task data structure
type Task struct {
	Name      string
	Run       func()
	fromQName string
}

// New Constructor
func New(opts ...Option) *MultiQ {
	mq := &MultiQ{
		Concurrency:  defaultConcurrency,
		logger:       log.New(os.Stderr, fmt.Sprintf("[%v] ", os.Getpid()), log.Ldate|log.Lmicroseconds),
		qConcurUsage: make(map[string]int),
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
	tasksCh := make(chan *Task)

	for i := 0; i < mq.Concurrency; i++ {
		mq.dbug("+ spine up worker %v", i+1)
		go mq.worker(i, tasksCh)
	}

	idle := 0
	for idle < 5 {
		allEmpty := true
		for _, q := range mq.queues {

			allEmpty = allEmpty && q.IsEmpty()
			if allEmpty {
				mq.dbug("all empty after checking q [%s]: %v", q.Name, allEmpty)
				continue
			}

			mq.dbug("queue [%s] concurrency usage: [%v]", q.Name, mq.qConcurUsage[q.Name])
			if mq.qConcurUsage[q.Name] >= q.Concurrency {
				mq.dbug("hit the limit of concurrency [%v] for queue [%s]", q.Concurrency, q.Name)
				continue
			}

			task, ok := q.Dequeue()
			if !ok {
				mq.logger.Printf("queue [%s] is empty", q.Name)
				continue
			}
			mq.dbug("<- task %v dequeued", task.Name)
			task.fromQName = q.Name

			mq.qConcurUsage[q.Name]++
			tasksCh <- task

			idle = 0
		}

		if allEmpty {
			mq.logger.Printf("All queues are empty, running idle [%v] ...", idle)
			d := time.Duration(time.Duration((idle+1)*3) * time.Second)
			time.Sleep(d)
			idle++
		} else {
			time.Sleep(time.Duration(time.Duration(500) * time.Millisecond))
		}
	}

	close(tasksCh)
	mq.dbug("XX task channel closed")
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
		mq.qConcurUsage[task.fromQName]--
		mq.dbug("worker %d: task %v done", id, task.Name)
	}
}

func (mq *MultiQ) dbug(format string, args ...interface{}) {
	format = "DBUG - " + format
	mq.logger.Printf(format, args...)
}
