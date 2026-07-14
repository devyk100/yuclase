package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// Index entry format: [offset:8][position:8]
	IndexEntrySize = 16
)

type IndexEntry struct {
	Offset   int64 // Global offset in the log
	Position int64 // File position where the message starts
}

type Index struct {
	mu      sync.RWMutex
	entries []IndexEntry
	path    string
	file    *os.File
	dirty   bool
}

// NewIndex creates a new index
func NewIndex(path string) (*Index, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	index := &Index{
		path:    path,
		entries: make([]IndexEntry, 0),
	}

	// Load existing index if it exists
	if err := index.load(); err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return index, nil
}

// AddEntry adds an entry to the index
func (i *Index) AddEntry(offset int64, position int64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	entry := IndexEntry{
		Offset:   offset,
		Position: position,
	}

	// Insert in sorted order
	insertPos := sort.Search(len(i.entries), func(j int) bool {
		return i.entries[j].Offset >= offset
	})

	// Check if entry already exists
	if insertPos < len(i.entries) && i.entries[insertPos].Offset == offset {
		i.entries[insertPos] = entry
	} else {
		// Insert new entry
		i.entries = append(i.entries, IndexEntry{})
		copy(i.entries[insertPos+1:], i.entries[insertPos:])
		i.entries[insertPos] = entry
	}

	i.dirty = true
	return nil
}

// GetPosition gets the position of a message in the log based on offset
func (i *Index) GetPosition(offset int64) (int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Binary search for the entry
	pos := sort.Search(len(i.entries), func(j int) bool {
		return i.entries[j].Offset >= offset
	})

	if pos < len(i.entries) && i.entries[pos].Offset == offset {
		return i.entries[pos].Position, nil
	}

	return 0, fmt.Errorf("offset %d not found in index", offset)
}

// GetClosestPosition finds the closest position for a given offset
// Returns the position of the largest offset <= target offset
func (i *Index) GetClosestPosition(offset int64) (int64, int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if len(i.entries) == 0 {
		return 0, 0, fmt.Errorf("index is empty")
	}

	// Binary search for the largest offset <= target
	pos := sort.Search(len(i.entries), func(j int) bool {
		return i.entries[j].Offset > offset
	})

	if pos == 0 {
		// All entries are greater than target
		return i.entries[0].Offset, i.entries[0].Position, nil
	}

	// Return the previous entry (largest <= target)
	pos--
	return i.entries[pos].Offset, i.entries[pos].Position, nil
}

// GetRange returns all entries in a range
func (i *Index) GetRange(startOffset, endOffset int64) ([]IndexEntry, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var result []IndexEntry

	for _, entry := range i.entries {
		if entry.Offset >= startOffset && entry.Offset <= endOffset {
			result = append(result, entry)
		}
		if entry.Offset > endOffset {
			break
		}
	}

	return result, nil
}

// Sync persists the index to disk
func (i *Index) Sync() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.dirty {
		return nil
	}

	return i.persist()
}

// Close closes the index and persists any changes
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.dirty {
		if err := i.persist(); err != nil {
			return err
		}
	}

	if i.file != nil {
		err := i.file.Close()
		i.file = nil
		return err
	}

	return nil
}

// Size returns the number of entries in the index
func (i *Index) Size() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.entries)
}

// GetLastOffset returns the last offset in the index
func (i *Index) GetLastOffset() (int64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if len(i.entries) == 0 {
		return 0, fmt.Errorf("index is empty")
	}

	return i.entries[len(i.entries)-1].Offset, nil
}

// load loads the index from disk
func (i *Index) load() error {
	file, err := os.OpenFile(i.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	i.file = file

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		// Empty file, nothing to load
		return nil
	}

	// Read all entries
	numEntries := stat.Size() / IndexEntrySize
	i.entries = make([]IndexEntry, numEntries)

	for j := int64(0); j < numEntries; j++ {
		buf := make([]byte, IndexEntrySize)
		if _, err := file.ReadAt(buf, j*IndexEntrySize); err != nil {
			return fmt.Errorf("failed to read index entry %d: %w", j, err)
		}

		i.entries[j] = IndexEntry{
			Offset:   int64(binary.BigEndian.Uint64(buf[0:8])),
			Position: int64(binary.BigEndian.Uint64(buf[8:16])),
		}
	}

	return nil
}

// persist saves the index to disk
func (i *Index) persist() error {
	if i.file == nil {
		file, err := os.OpenFile(i.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		i.file = file
	}

	// Truncate file
	if err := i.file.Truncate(0); err != nil {
		return err
	}

	// Seek to beginning
	if _, err := i.file.Seek(0, 0); err != nil {
		return err
	}

	// Write all entries
	for _, entry := range i.entries {
		buf := make([]byte, IndexEntrySize)
		binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
		binary.BigEndian.PutUint64(buf[8:16], uint64(entry.Position))

		if _, err := i.file.Write(buf); err != nil {
			return fmt.Errorf("failed to write index entry: %w", err)
		}
	}

	// Sync to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	i.dirty = false
	return nil
}

// Rebuild rebuilds the index by scanning the log
func (i *Index) Rebuild(log *Log) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Clear existing entries
	i.entries = i.entries[:0]

	// Get all log segments
	segments, err := log.ListSegments()
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	// Scan each segment
	for segmentIndex, segmentPath := range segments {
		if err := i.scanSegment(segmentPath, segmentIndex); err != nil {
			return fmt.Errorf("failed to scan segment %s: %w", segmentPath, err)
		}
	}

	i.dirty = true
	return i.persist()
}

// scanSegment scans a single log segment and adds entries to the index
func (i *Index) scanSegment(segmentPath string, segmentIndex int) error {
	file, err := os.Open(segmentPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var position int64 = 0

	for {
		// Read message header
		header := make([]byte, MessageHeaderSize)
		n, err := file.Read(header)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		if n != MessageHeaderSize {
			break
		}

		// Parse header
		dataSize := binary.BigEndian.Uint32(header[8:12])

		// Calculate global offset
		globalOffset := int64(segmentIndex)<<32 | position

		// Add to index
		i.entries = append(i.entries, IndexEntry{
			Offset:   globalOffset,
			Position: position,
		})

		// Skip message data
		if _, err := file.Seek(int64(dataSize), 1); err != nil {
			return err
		}

		position += int64(MessageHeaderSize + dataSize)
	}

	return nil
}
