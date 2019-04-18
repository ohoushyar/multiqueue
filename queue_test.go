package multiqueue

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func TestNewQ(t *testing.T) {

	t.Run("Default NewQ", func(t *testing.T) {
		q := NewQ("foo")
		if q == nil {
			t.FailNow()
		}
	})

	t.Run("With options", func(t *testing.T) {
		expCon := 5
		debug := 1

		q := NewQ(
			"foo",
			WithQueueConcurrency(expCon),
			WithQueueDebug(debug),
			WithQueueLogger(log.New(os.Stderr, fmt.Sprintf("[%v] ", os.Getpid()), log.Ldate)),
		)
		if q == nil {
			t.Errorf("failed to NewQ with option")
		}
	})
}

func TestEnqueu(t *testing.T) {

	t.Run("Simple enqueue", func(t *testing.T) {
		q := NewQ("foo")

		q.Enqueue(&Task{Name: "T1", Run: func() { return }})
		q.Enqueue(&Task{Name: "T2", Run: func() { return }})

		if q.IsEmpty() {
			t.Error("returned empty after enqueue")
		}
	})

	t.Run("Concurrent enqueue", func(t *testing.T) {
		q := NewQ("foo")

		go func() {
			q.Enqueue(&Task{Name: "T1", Run: func() { return }})
		}()

		go func() {
			q.Enqueue(&Task{Name: "T2", Run: func() { return }})
		}()

		time.Sleep(time.Millisecond)
		if q.IsEmpty() {
			t.Error("returned empty after enqueue")
		}
	})
}
