package queue

import (
	"fmt"
	"sync"
	"time"

	"yuclase/pkg/storage"
)

type Consumer struct {
	mu           sync.RWMutex
	id           string
	topicName    string
	queue        *Queue
	offset       int64
	lastActivity time.Time
	active       bool
	messageChan  chan *storage.LogEntry
	errorChan    chan error
	stopChan     chan bool
	stopped      bool
}

type ConsumerInfo struct {
	ID           string    `json:"id"`
	TopicName    string    `json:"topic_name"`
	Offset       int64     `json:"offset"`
	LastActivity time.Time `json:"last_activity"`
	Active       bool      `json:"active"`
}

// NewConsumer creates a new consumer
func NewConsumer(id string, topicName string, queue *Queue) (*Consumer, error) {
	if id == "" {
		return nil, fmt.Errorf("consumer ID cannot be empty")
	}

	if topicName == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	// Get current offset for this consumer
	offset, err := queue.GetConsumerOffset(topicName, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer offset: %w", err)
	}

	consumer := &Consumer{
		id:           id,
		topicName:    topicName,
		queue:        queue,
		offset:       offset,
		lastActivity: time.Now(),
		active:       false,
		messageChan:  make(chan *storage.LogEntry, 100), // Buffered channel
		errorChan:    make(chan error, 10),
		stopChan:     make(chan bool, 1),
	}

	return consumer, nil
}

// Start starts the consumer to listen for messages
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active {
		return fmt.Errorf("consumer %s is already active", c.id)
	}

	if c.stopped {
		return fmt.Errorf("consumer %s has been stopped", c.id)
	}

	c.active = true
	c.lastActivity = time.Now()

	// Start the consumption loop in a goroutine
	go c.consumeLoop()

	return nil
}

// Stop stops the consumer
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return nil // Already stopped
	}

	c.active = false
	c.stopped = true

	// Signal stop
	select {
	case c.stopChan <- true:
	default:
	}

	// Close channels
	close(c.messageChan)
	close(c.errorChan)

	return nil
}

// GetMessages returns the message channel for receiving messages
func (c *Consumer) GetMessages() <-chan *storage.LogEntry {
	return c.messageChan
}

// GetErrors returns the error channel for receiving errors
func (c *Consumer) GetErrors() <-chan error {
	return c.errorChan
}

// CommitOffset commits the current offset
func (c *Consumer) CommitOffset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.queue.UpdateConsumerOffset(c.topicName, c.id, c.offset); err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	c.lastActivity = time.Now()
	return nil
}

// SetOffset sets the consumer's offset manually
func (c *Consumer) SetOffset(offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.offset = offset
	c.lastActivity = time.Now()

	return c.queue.UpdateConsumerOffset(c.topicName, c.id, offset)
}

// GetOffset returns the current offset
func (c *Consumer) GetOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.offset
}

// GetInfo returns consumer information
func (c *Consumer) GetInfo() *ConsumerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &ConsumerInfo{
		ID:           c.id,
		TopicName:    c.topicName,
		Offset:       c.offset,
		LastActivity: c.lastActivity,
		Active:       c.active,
	}
}

// IsActive returns true if the consumer is active
func (c *Consumer) IsActive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.active
}

// GetID returns the consumer ID
func (c *Consumer) GetID() string {
	return c.id
}

// GetTopicName returns the topic name
func (c *Consumer) GetTopicName() string {
	return c.topicName
}

// consumeLoop is the main consumption loop
func (c *Consumer) consumeLoop() {
	ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return

		case <-ticker.C:
			if err := c.pollMessages(); err != nil {
				// Send error to error channel
				select {
				case c.errorChan <- err:
				default:
					// Error channel is full, skip
				}
			}
		}
	}
}

// pollMessages polls for new messages
func (c *Consumer) pollMessages() error {
	c.mu.Lock()
	currentOffset := c.offset
	c.mu.Unlock()

	// Get messages from the queue
	messages, err := c.queue.DequeueWithLimit(c.topicName, c.id, 10)
	if err != nil {
		return fmt.Errorf("failed to dequeue messages: %w", err)
	}

	// Process messages
	for _, message := range messages {
		// Skip messages we've already processed
		if message.Offset <= currentOffset {
			continue
		}

		// Send message to channel
		select {
		case c.messageChan <- message:
			// Update offset
			c.mu.Lock()
			c.offset = message.Offset
			c.lastActivity = time.Now()
			c.mu.Unlock()

		default:
			// Message channel is full, stop processing for now
			return nil
		}
	}

	return nil
}

// Reset resets the consumer to start from the beginning
func (c *Consumer) Reset() error {
	return c.SetOffset(0)
}

// SeekToEnd seeks to the end of the topic
func (c *Consumer) SeekToEnd() error {
	// Get topic stats to find the latest offset
	stats, err := c.queue.GetTopicStats(c.topicName)
	if err != nil {
		return fmt.Errorf("failed to get topic stats: %w", err)
	}

	// Set offset to the end
	return c.SetOffset(stats.Size)
}

// SeekToTimestamp seeks to a specific timestamp (approximate)
func (c *Consumer) SeekToTimestamp(timestamp time.Time) error {
	// This is a simplified implementation
	// In a real system, you'd use the index to find the closest offset to the timestamp

	// For now, just reset to beginning if timestamp is old, or seek to end if recent
	if timestamp.Before(time.Now().Add(-24 * time.Hour)) {
		return c.Reset()
	}

	return c.SeekToEnd()
}

// GetLag returns the consumer lag (difference between latest offset and consumer offset)
func (c *Consumer) GetLag() (int64, error) {
	stats, err := c.queue.GetTopicStats(c.topicName)
	if err != nil {
		return 0, fmt.Errorf("failed to get topic stats: %w", err)
	}

	c.mu.RLock()
	currentOffset := c.offset
	c.mu.RUnlock()

	lag := stats.Size - currentOffset
	if lag < 0 {
		lag = 0
	}

	return lag, nil
}

// UpdateActivity updates the last activity timestamp
func (c *Consumer) UpdateActivity() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastActivity = time.Now()
}

// GetLastActivity returns the last activity timestamp
func (c *Consumer) GetLastActivity() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastActivity
}

// IsStale returns true if the consumer hasn't been active for a specified duration
func (c *Consumer) IsStale(maxAge time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return time.Since(c.lastActivity) > maxAge
}
