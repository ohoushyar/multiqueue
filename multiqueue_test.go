package multiqueue

import (
	"fmt"
	"testing"
)

func TestRun(t *testing.T) {
	mq := New()
	mq.Run()
}

func TestAddQueue(t *testing.T) {

	t.Run("Simple add", func(t *testing.T) {
		mq := New()
		for i := 1; i <= 2; i++ {
			q := NewQ(fmt.Sprintf("q%d", i))
			if err := mq.AddQueue(q); err != nil {
				t.Errorf("failed! Expect to add new queue")
			}
		}
	})

	t.Run("Error on duplicate queue", func(t *testing.T) {
		mq := New()
		q := NewQ("q1")
		if err := mq.AddQueue(q); err != nil {
			t.Errorf("failed! Expect to add new queue")
		}

		if err := mq.AddQueue(q); err == nil {
			t.Errorf("failed! Expect to throw err on duplicate queue")
		}
	})
}
