package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"yuclase/internal/config"
	"yuclase/pkg/storage"
)

type Queue struct {
	mu            sync.RWMutex
	topics        map[string]*Topic
	config        *config.Config
	cleanupTicker *time.Ticker
	cleanupDone   chan bool
	ctx           context.Context
	cancel        context.CancelFunc
	closed        bool
}

type QueueStats struct {
	TopicCount    int                    `json:"topic_count"`
	TotalMessages int64                  `json:"total_messages"`
	TotalSize     int64                  `json:"total_size"`
	Topics        map[string]*TopicStats `json:"topics"`
}

// NewQueue creates a new queue instance
func NewQueue(config *config.Config) (*Queue, error) {
	if err := os.MkdirAll(config.Storage.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	queue := &Queue{
		topics: make(map[string]*Topic),
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	// Load existing topics
	if err := queue.loadExistingTopics(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load existing topics: %w", err)
	}

	// Start background cleanup
	queue.startCleanup()

	return queue, nil
}

// Enqueue adds a message to a topic
func (q *Queue) Enqueue(topicName string, data []byte) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return 0, fmt.Errorf("queue is closed")
	}

	topic, err := q.getOrCreateTopic(topicName)
	if err != nil {
		return 0, fmt.Errorf("failed to get topic %s: %w", topicName, err)
	}

	return topic.Enqueue(data)
}

// Dequeue reads messages for a consumer from a topic
func (q *Queue) Dequeue(topicName string, consumerID string) ([]*storage.LogEntry, error) {
	return q.DequeueWithLimit(topicName, consumerID, 10) // Default limit
}

// DequeueWithLimit reads messages for a consumer with a specified limit
func (q *Queue) DequeueWithLimit(topicName string, consumerID string, maxMessages int) ([]*storage.LogEntry, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, fmt.Errorf("queue is closed")
	}

	topic, exists := q.topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	return topic.Dequeue(consumerID, maxMessages)
}

// UpdateConsumerOffset updates the offset for a consumer
func (q *Queue) UpdateConsumerOffset(topicName string, consumerID string, offset int64) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	topic, exists := q.topics[topicName]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	return topic.UpdateConsumerOffset(consumerID, offset)
}

// GetConsumerOffset gets the current offset for a consumer
func (q *Queue) GetConsumerOffset(topicName string, consumerID string) (int64, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	topic, exists := q.topics[topicName]
	if !exists {
		return 0, fmt.Errorf("topic %s does not exist", topicName)
	}

	return topic.GetConsumerOffset(consumerID)
}

// CreateTopic explicitly creates a new topic
func (q *Queue) CreateTopic(topicName string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	if _, exists := q.topics[topicName]; exists {
		return fmt.Errorf("topic %s already exists", topicName)
	}

	topic, err := NewTopic(topicName, q.config)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topicName, err)
	}

	q.topics[topicName] = topic
	return nil
}

// DeleteTopic removes a topic and all its data
func (q *Queue) DeleteTopic(topicName string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	topic, exists := q.topics[topicName]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	// Close and delete the topic
	if err := topic.Delete(); err != nil {
		return fmt.Errorf("failed to delete topic %s: %w", topicName, err)
	}

	delete(q.topics, topicName)
	return nil
}

// ListTopics returns all topic names
func (q *Queue) ListTopics() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()

	topics := make([]string, 0, len(q.topics))
	for name := range q.topics {
		topics = append(topics, name)
	}

	return topics
}

// GetTopicStats returns statistics for a specific topic
func (q *Queue) GetTopicStats(topicName string) (*TopicStats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	topic, exists := q.topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	return topic.GetStats()
}

// GetQueueStats returns overall queue statistics
func (q *Queue) GetQueueStats() (*QueueStats, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	stats := &QueueStats{
		TopicCount: len(q.topics),
		Topics:     make(map[string]*TopicStats),
	}

	for name, topic := range q.topics {
		topicStats, err := topic.GetStats()
		if err != nil {
			continue // Skip topics with errors
		}

		stats.Topics[name] = topicStats
		stats.TotalMessages += topicStats.MessageCount
		stats.TotalSize += topicStats.Size
	}

	return stats, nil
}

// ListConsumers returns all consumers for a topic
func (q *Queue) ListConsumers(topicName string) ([]string, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	topic, exists := q.topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	return topic.ListConsumers(), nil
}

// Sync forces synchronization of all topics to disk
func (q *Queue) Sync() error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	var errors []error
	for name, topic := range q.topics {
		if err := topic.Sync(); err != nil {
			errors = append(errors, fmt.Errorf("failed to sync topic %s: %w", name, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("sync errors: %v", errors)
	}

	return nil
}

// Close closes the queue and all its topics
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	// Stop cleanup
	q.cancel()
	if q.cleanupTicker != nil {
		q.cleanupTicker.Stop()
	}
	if q.cleanupDone != nil {
		<-q.cleanupDone
	}

	// Close all topics
	var errors []error
	for name, topic := range q.topics {
		if err := topic.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close topic %s: %w", name, err))
		}
	}

	q.closed = true

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	return nil
}

// getOrCreateTopic gets an existing topic or creates a new one
func (q *Queue) getOrCreateTopic(topicName string) (*Topic, error) {
	if topic, exists := q.topics[topicName]; exists {
		return topic, nil
	}

	// Create new topic
	topic, err := NewTopic(topicName, q.config)
	if err != nil {
		return nil, err
	}

	q.topics[topicName] = topic
	return topic, nil
}

// loadExistingTopics loads all existing topics from disk
func (q *Queue) loadExistingTopics() error {
	topicsDir := filepath.Join(q.config.Storage.DataDirectory, "topics")

	// Check if topics directory exists
	if _, err := os.Stat(topicsDir); os.IsNotExist(err) {
		return nil // No topics to load
	}

	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		return fmt.Errorf("failed to read topics directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		topic, err := LoadTopic(topicName, q.config)
		if err != nil {
			// Log error but continue loading other topics
			continue
		}

		q.topics[topicName] = topic
	}

	return nil
}

// startCleanup starts the background cleanup process
func (q *Queue) startCleanup() {
	q.cleanupTicker = time.NewTicker(q.config.GetCleanupInterval())
	q.cleanupDone = make(chan bool)

	go func() {
		defer func() {
			q.cleanupDone <- true
		}()

		for {
			select {
			case <-q.ctx.Done():
				return
			case <-q.cleanupTicker.C:
				q.performCleanup()
			}
		}
	}()
}

// performCleanup performs periodic cleanup of old messages
func (q *Queue) performCleanup() {
	q.mu.RLock()
	topics := make([]*Topic, 0, len(q.topics))
	for _, topic := range q.topics {
		topics = append(topics, topic)
	}
	q.mu.RUnlock()

	// Cleanup each topic
	for _, topic := range topics {
		if err := topic.CleanupOldMessages(); err != nil {
			// Log error but continue with other topics
			continue
		}
	}
}

// TopicExists checks if a topic exists
func (q *Queue) TopicExists(topicName string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	_, exists := q.topics[topicName]
	return exists
}

// GetTopicCount returns the number of topics
func (q *Queue) GetTopicCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.topics)
}

// IsEmpty returns true if the queue has no topics
func (q *Queue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.topics) == 0
}

// Reset removes all topics and their data
func (q *Queue) Reset() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	// Close and delete all topics
	var errors []error
	for name, topic := range q.topics {
		if err := topic.Delete(); err != nil {
			errors = append(errors, fmt.Errorf("failed to delete topic %s: %w", name, err))
		}
	}

	// Clear topics map
	q.topics = make(map[string]*Topic)

	if len(errors) > 0 {
		return fmt.Errorf("reset errors: %v", errors)
	}

	return nil
}
