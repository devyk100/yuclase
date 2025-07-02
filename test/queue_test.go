// test/queue_test.go
package test

import (
    "testing"
    "yuclase/pkg/queue"
)

func TestQueueEnqueue(t *testing.T) {
    q := queue.NewQueue()
    err := q.Enqueue("test-topic", []byte("test message"))
    if err != nil {
        t.Errorf("Failed to enqueue message: %v", err)
    }
}

func TestQueueDequeue(t *testing.T) {
    q := queue.NewQueue()
    _, err := q.Dequeue("test-topic", "consumer-id")
    if err != nil {
        t.Errorf("Failed to dequeue message: %v", err)
    }
}
