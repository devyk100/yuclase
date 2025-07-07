package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"yuclase/internal/config"
	"yuclase/pkg/storage"
)

type Topic struct {
	mu       sync.RWMutex
	name     string
	path     string
	log      *storage.Log
	index    *storage.Index
	catalog  *storage.Catalog
	config   *config.Config
	created  time.Time
	lastUsed time.Time
	closed   bool
}

type TopicStats struct {
	Name         string    `json:"name"`
	MessageCount int64     `json:"message_count"`
	Size         int64     `json:"size"`
	Consumers    int       `json:"consumers"`
	Created      time.Time `json:"created"`
	LastUsed     time.Time `json:"last_used"`
}

// NewTopic creates a new topic
func NewTopic(name string, config *config.Config) (*Topic, error) {
	if name == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	// Create topic directory
	topicPath := filepath.Join(config.Storage.DataDirectory, "topics", name)
	if err := os.MkdirAll(topicPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}

	topic := &Topic{
		name:     name,
		path:     topicPath,
		config:   config,
		created:  time.Now(),
		lastUsed: time.Now(),
	}

	if err := topic.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize topic: %w", err)
	}

	return topic, nil
}

// LoadTopic loads an existing topic
func LoadTopic(name string, config *config.Config) (*Topic, error) {
	topicPath := filepath.Join(config.Storage.DataDirectory, "topics", name)

	// Check if topic directory exists
	if _, err := os.Stat(topicPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("topic %s does not exist", name)
	}

	topic := &Topic{
		name:     name,
		path:     topicPath,
		config:   config,
		lastUsed: time.Now(),
	}

	if err := topic.initialize(); err != nil {
		return nil, fmt.Errorf("failed to load topic: %w", err)
	}

	// Load creation time from metadata if available
	topic.loadMetadata()

	return topic, nil
}

// initialize sets up the topic's storage components
func (t *Topic) initialize() error {
	// Initialize log
	logPath := filepath.Join(t.path, "data.log")
	log, err := storage.NewLog(logPath, t.config.GetLogSegmentBytes(), t.config.GetSyncInterval())
	if err != nil {
		return fmt.Errorf("failed to create log: %w", err)
	}
	t.log = log

	// Initialize index
	indexPath := filepath.Join(t.path, "index.idx")
	index, err := storage.NewIndex(indexPath)
	if err != nil {
		t.log.Close()
		return fmt.Errorf("failed to create index: %w", err)
	}
	t.index = index

	// Initialize catalog
	catalogPath := filepath.Join(t.path, "consumers.json")
	catalog, err := storage.NewCatalog(catalogPath)
	if err != nil {
		t.log.Close()
		t.index.Close()
		return fmt.Errorf("failed to create catalog: %w", err)
	}
	t.catalog = catalog

	return nil
}

// Enqueue adds a message to the topic
func (t *Topic) Enqueue(data []byte) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0, fmt.Errorf("topic %s is closed", t.name)
	}

	// Append to log
	offset, err := t.log.Append(data)
	if err != nil {
		return 0, fmt.Errorf("failed to append to log: %w", err)
	}

	// Add to index (every nth message based on config)
	if t.shouldIndex(offset) {
		if err := t.index.AddEntry(offset, offset); err != nil {
			// Log error but don't fail the enqueue
			// Index can be rebuilt if needed
		}
	}

	t.lastUsed = time.Now()
	return offset, nil
}

// Dequeue reads messages for a consumer starting from their last offset
func (t *Topic) Dequeue(consumerID string, maxMessages int) ([]*storage.LogEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, fmt.Errorf("topic %s is closed", t.name)
	}

	// Get consumer's current offset
	offset, err := t.catalog.GetOffset(consumerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer offset: %w", err)
	}

	// Read messages from log
	entries, err := t.readFromOffset(offset, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read from log: %w", err)
	}

	// Update consumer offset to point to the next unread message
	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		nextOffset := lastEntry.Offset + int64(storage.MessageHeaderSize+len(lastEntry.Data))
		if err := t.catalog.UpdateOffset(consumerID, nextOffset); err != nil {
			return nil, fmt.Errorf("failed to update consumer offset: %w", err)
		}
	}

	t.lastUsed = time.Now()
	return entries, nil
}

// UpdateConsumerOffset updates the offset for a consumer
func (t *Topic) UpdateConsumerOffset(consumerID string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("topic %s is closed", t.name)
	}

	return t.catalog.UpdateOffset(consumerID, offset)
}

// GetConsumerOffset gets the current offset for a consumer
func (t *Topic) GetConsumerOffset(consumerID string) (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.catalog.GetOffset(consumerID)
}

// ListConsumers returns all consumers for this topic
func (t *Topic) ListConsumers() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.catalog.ListConsumers()
}

// GetStats returns statistics about the topic
func (t *Topic) GetStats() (*TopicStats, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return &TopicStats{
		Name:         t.name,
		MessageCount: t.getMessageCount(),
		Size:         t.log.GetSize(),
		Consumers:    t.catalog.Size(),
		Created:      t.created,
		LastUsed:     t.lastUsed,
	}, nil
}

// Sync forces synchronization of all components to disk
func (t *Topic) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("topic %s is closed", t.name)
	}

	// Sync all components
	if err := t.log.Sync(); err != nil {
		return fmt.Errorf("failed to sync log: %w", err)
	}

	if err := t.index.Sync(); err != nil {
		return fmt.Errorf("failed to sync index: %w", err)
	}

	if err := t.catalog.Sync(); err != nil {
		return fmt.Errorf("failed to sync catalog: %w", err)
	}

	// Save metadata
	return t.saveMetadata()
}

// Close closes the topic and all its components
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	var errors []error

	// Close all components
	if t.log != nil {
		if err := t.log.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close log: %w", err))
		}
	}

	if t.index != nil {
		if err := t.index.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close index: %w", err))
		}
	}

	if t.catalog != nil {
		if err := t.catalog.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close catalog: %w", err))
		}
	}

	// Save metadata
	if err := t.saveMetadata(); err != nil {
		errors = append(errors, fmt.Errorf("failed to save metadata: %w", err))
	}

	t.closed = true

	if len(errors) > 0 {
		return fmt.Errorf("errors closing topic: %v", errors)
	}

	return nil
}

// Delete removes the topic and all its data
func (t *Topic) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Close first
	if !t.closed {
		t.Close()
	}

	// Remove directory
	return os.RemoveAll(t.path)
}

// CleanupOldMessages removes messages older than the retention period
func (t *Topic) CleanupOldMessages() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("topic %s is closed", t.name)
	}

	retentionTime := time.Now().Add(-t.config.GetRetentionDuration())

	// Get all log segments
	segments, err := t.log.ListSegments()
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	// Find segments to delete
	var toDelete []string
	for _, segment := range segments {
		stat, err := os.Stat(segment)
		if err != nil {
			continue
		}

		if stat.ModTime().Before(retentionTime) {
			toDelete = append(toDelete, segment)
		}
	}

	// Delete old segments
	for _, segment := range toDelete {
		if err := t.log.DeleteSegment(segment); err != nil {
			return fmt.Errorf("failed to delete segment %s: %w", segment, err)
		}
	}

	// Rebuild index if segments were deleted
	if len(toDelete) > 0 {
		if err := t.index.Rebuild(t.log); err != nil {
			return fmt.Errorf("failed to rebuild index: %w", err)
		}
	}

	return nil
}

// GetName returns the topic name
func (t *Topic) GetName() string {
	return t.name
}

// IsEmpty returns true if the topic has no messages
func (t *Topic) IsEmpty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.log.GetSize() == 0
}

// shouldIndex determines if a message should be indexed
func (t *Topic) shouldIndex(offset int64) bool {
	return offset%int64(t.config.Storage.IndexInterval) == 0
}

// readFromOffset reads messages starting from an offset
func (t *Topic) readFromOffset(offset int64, maxMessages int) ([]*storage.LogEntry, error) {
	// Use the log's ReadFrom method to get all messages from offset
	allEntries, err := t.log.ReadFrom(offset)
	if err != nil {
		return nil, err
	}

	// Limit to maxMessages
	if len(allEntries) > maxMessages {
		return allEntries[:maxMessages], nil
	}

	return allEntries, nil
}

// getMessageCount estimates the number of messages in the topic
func (t *Topic) getMessageCount() int64 {
	// This is an approximation based on log size and average message size
	// For exact count, we'd need to scan the entire log
	size := t.log.GetSize()
	if size == 0 {
		return 0
	}

	// Estimate based on minimum message size
	return size / int64(storage.MessageHeaderSize+1)
}

// loadMetadata loads topic metadata
func (t *Topic) loadMetadata() {
	// Implementation would load creation time and other metadata
	// For now, use current time as fallback
	if t.created.IsZero() {
		t.created = time.Now()
	}
}

// saveMetadata saves topic metadata
func (t *Topic) saveMetadata() error {
	// Implementation would save creation time and other metadata
	// For now, this is a placeholder
	return nil
}
