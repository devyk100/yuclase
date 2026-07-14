package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ConsumerOffset struct {
	ConsumerID  string    `json:"consumer_id"`
	Offset      int64     `json:"offset"`
	LastUpdated time.Time `json:"last_updated"`
	LastCommit  time.Time `json:"last_commit"`
}

type Catalog struct {
	mu        sync.RWMutex
	path      string
	consumers map[string]*ConsumerOffset
	dirty     bool
}

// NewCatalog creates a new catalog for consumer offset management
func NewCatalog(path string) (*Catalog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create catalog directory: %w", err)
	}

	catalog := &Catalog{
		path:      path,
		consumers: make(map[string]*ConsumerOffset),
	}

	// Load existing catalog if it exists
	if err := catalog.load(); err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	return catalog, nil
}

// UpdateOffset updates the offset for a consumer
func (c *Catalog) UpdateOffset(consumerID string, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	if existing, exists := c.consumers[consumerID]; exists {
		existing.Offset = offset
		existing.LastUpdated = now
	} else {
		c.consumers[consumerID] = &ConsumerOffset{
			ConsumerID:  consumerID,
			Offset:      offset,
			LastUpdated: now,
			LastCommit:  now,
		}
	}

	c.dirty = true
	return nil
}

// GetOffset gets the offset for a consumer
func (c *Catalog) GetOffset(consumerID string) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if consumer, exists := c.consumers[consumerID]; exists {
		return consumer.Offset, nil
	}

	return 0, nil // Start from beginning if consumer doesn't exist
}

// CommitOffset commits the current offset for a consumer
func (c *Catalog) CommitOffset(consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if consumer, exists := c.consumers[consumerID]; exists {
		consumer.LastCommit = time.Now()
		c.dirty = true
		return nil
	}

	return fmt.Errorf("consumer %s not found", consumerID)
}

// GetConsumerInfo returns detailed information about a consumer
func (c *Catalog) GetConsumerInfo(consumerID string) (*ConsumerOffset, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if consumer, exists := c.consumers[consumerID]; exists {
		// Return a copy to avoid race conditions
		return &ConsumerOffset{
			ConsumerID:  consumer.ConsumerID,
			Offset:      consumer.Offset,
			LastUpdated: consumer.LastUpdated,
			LastCommit:  consumer.LastCommit,
		}, nil
	}

	return nil, fmt.Errorf("consumer %s not found", consumerID)
}

// ListConsumers returns all consumer IDs
func (c *Catalog) ListConsumers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	consumers := make([]string, 0, len(c.consumers))
	for consumerID := range c.consumers {
		consumers = append(consumers, consumerID)
	}

	return consumers
}

// GetAllConsumers returns all consumer information
func (c *Catalog) GetAllConsumers() map[string]*ConsumerOffset {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*ConsumerOffset)
	for id, consumer := range c.consumers {
		result[id] = &ConsumerOffset{
			ConsumerID:  consumer.ConsumerID,
			Offset:      consumer.Offset,
			LastUpdated: consumer.LastUpdated,
			LastCommit:  consumer.LastCommit,
		}
	}

	return result
}

// RemoveConsumer removes a consumer from the catalog
func (c *Catalog) RemoveConsumer(consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.consumers[consumerID]; exists {
		delete(c.consumers, consumerID)
		c.dirty = true
		return nil
	}

	return fmt.Errorf("consumer %s not found", consumerID)
}

// CleanupStaleConsumers removes consumers that haven't been active for a specified duration
func (c *Catalog) CleanupStaleConsumers(maxAge time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var removed []string

	for consumerID, consumer := range c.consumers {
		if now.Sub(consumer.LastUpdated) > maxAge {
			delete(c.consumers, consumerID)
			removed = append(removed, consumerID)
		}
	}

	if len(removed) > 0 {
		c.dirty = true
	}

	return nil
}

// Sync persists the catalog to disk
func (c *Catalog) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.dirty {
		return nil
	}

	return c.persist()
}

// Close closes the catalog and persists any changes
func (c *Catalog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dirty {
		return c.persist()
	}

	return nil
}

// Size returns the number of consumers in the catalog
func (c *Catalog) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.consumers)
}

// GetMinOffset returns the minimum offset across all consumers
func (c *Catalog) GetMinOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.consumers) == 0 {
		return 0
	}

	var minOffset int64 = -1
	for _, consumer := range c.consumers {
		if minOffset == -1 || consumer.Offset < minOffset {
			minOffset = consumer.Offset
		}
	}

	return minOffset
}

// GetMaxOffset returns the maximum offset across all consumers
func (c *Catalog) GetMaxOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var maxOffset int64 = 0
	for _, consumer := range c.consumers {
		if consumer.Offset > maxOffset {
			maxOffset = consumer.Offset
		}
	}

	return maxOffset
}

// load loads the catalog from disk
func (c *Catalog) load() error {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, start with empty catalog
			return nil
		}
		return fmt.Errorf("failed to read catalog file: %w", err)
	}

	if len(data) == 0 {
		// Empty file
		return nil
	}

	var consumers map[string]*ConsumerOffset
	if err := json.Unmarshal(data, &consumers); err != nil {
		return fmt.Errorf("failed to unmarshal catalog: %w", err)
	}

	c.consumers = consumers
	return nil
}

// persist saves the catalog to disk
func (c *Catalog) persist() error {
	data, err := json.MarshalIndent(c.consumers, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	// Write to temporary file first
	tempPath := c.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary catalog file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, c.path); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename catalog file: %w", err)
	}

	c.dirty = false
	return nil
}

// Reset resets the catalog (removes all consumers)
func (c *Catalog) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consumers = make(map[string]*ConsumerOffset)
	c.dirty = true

	return c.persist()
}

// Export exports the catalog data for backup/migration
func (c *Catalog) Export() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return json.MarshalIndent(c.consumers, "", "  ")
}

// Import imports catalog data from backup/migration
func (c *Catalog) Import(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var consumers map[string]*ConsumerOffset
	if err := json.Unmarshal(data, &consumers); err != nil {
		return fmt.Errorf("failed to unmarshal import data: %w", err)
	}

	c.consumers = consumers
	c.dirty = true

	return c.persist()
}
