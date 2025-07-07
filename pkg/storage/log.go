package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// Message format: [timestamp:8][size:4][data:size]
	MessageHeaderSize = 12 // 8 bytes timestamp + 4 bytes size
)

type Log struct {
	mu           sync.RWMutex
	file         *os.File
	writer       *bufio.Writer
	path         string
	currentSize  int64
	maxSize      int64
	segmentIndex int
	lastSync     time.Time
	syncInterval time.Duration
}

type LogEntry struct {
	Timestamp int64
	Data      []byte
	Offset    int64
}

// NewLog creates a new append-only log
func NewLog(path string, maxSize int64, syncInterval time.Duration) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	log := &Log{
		path:         path,
		maxSize:      maxSize,
		syncInterval: syncInterval,
		lastSync:     time.Now(),
	}

	if err := log.openCurrentSegment(); err != nil {
		return nil, fmt.Errorf("failed to open log segment: %w", err)
	}

	return log, nil
}

// Append adds a new message to the log
func (l *Log) Append(data []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if we need to rotate to a new segment
	messageSize := int64(MessageHeaderSize + len(data))
	if l.currentSize+messageSize > l.maxSize {
		if err := l.rotateSegment(); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Get current offset before writing
	offset := l.currentSize

	// Write message header
	timestamp := time.Now().UnixNano()
	header := make([]byte, MessageHeaderSize)
	binary.BigEndian.PutUint64(header[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))

	if _, err := l.writer.Write(header); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}

	// Write message data
	if _, err := l.writer.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}

	l.currentSize += messageSize

	// Always sync after write to ensure data is available for reading
	if err := l.sync(); err != nil {
		return 0, fmt.Errorf("failed to sync: %w", err)
	}

	return l.getGlobalOffset(offset), nil
}

// Read reads a message from the log at a specific global offset
func (l *Log) Read(globalOffset int64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	segmentIndex, localOffset := l.parseGlobalOffset(globalOffset)
	segmentPath := l.getSegmentPath(segmentIndex)

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %d: %w", segmentIndex, err)
	}
	defer file.Close()

	// Seek to the local offset
	if _, err := file.Seek(localOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", localOffset, err)
	}

	// Read message header
	header := make([]byte, MessageHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	timestamp := int64(binary.BigEndian.Uint64(header[0:8]))
	dataSize := binary.BigEndian.Uint32(header[8:12])

	// Read message data
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	return &LogEntry{
		Timestamp: timestamp,
		Data:      data,
		Offset:    globalOffset,
	}, nil
}

// ReadFrom reads all messages starting from a global offset
func (l *Log) ReadFrom(globalOffset int64) ([]*LogEntry, error) {
	var entries []*LogEntry
	currentOffset := globalOffset

	for {
		entry, err := l.Read(currentOffset)
		if err != nil {
			// Check if it's EOF or any other read error
			if err == io.EOF || err.Error() == "failed to read header: EOF" {
				break
			}
			return nil, err
		}

		entries = append(entries, entry)
		currentOffset += int64(MessageHeaderSize + len(entry.Data))
	}

	return entries, nil
}

// Sync forces a sync to disk
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sync()
}

func (l *Log) sync() error {
	if err := l.writer.Flush(); err != nil {
		return err
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.lastSync = time.Now()
	return nil
}

// Close closes the log
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.sync(); err != nil {
		return err
	}

	if l.writer != nil {
		l.writer = nil
	}

	if l.file != nil {
		err := l.file.Close()
		l.file = nil
		return err
	}

	return nil
}

// GetSize returns the current size of the log
func (l *Log) GetSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getGlobalOffset(l.currentSize)
}

// openCurrentSegment opens the current log segment for writing
func (l *Log) openCurrentSegment() error {
	// Find the latest segment
	l.segmentIndex = l.findLatestSegment()
	segmentPath := l.getSegmentPath(l.segmentIndex)

	// Open or create the segment file
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Get current size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	l.file = file
	l.writer = bufio.NewWriter(file)
	l.currentSize = stat.Size()

	return nil
}

// rotateSegment creates a new log segment
func (l *Log) rotateSegment() error {
	// Sync current segment
	if err := l.sync(); err != nil {
		return err
	}

	// Close current segment
	if l.file != nil {
		l.file.Close()
	}

	// Create new segment
	l.segmentIndex++
	l.currentSize = 0

	return l.openCurrentSegment()
}

// findLatestSegment finds the latest segment index
func (l *Log) findLatestSegment() int {
	dir := filepath.Dir(l.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	maxIndex := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) == ".log" {
			var index int
			if _, err := fmt.Sscanf(name, "%018d.log", &index); err == nil {
				if index > maxIndex {
					maxIndex = index
				}
			}
		}
	}

	return maxIndex
}

// getSegmentPath returns the path for a segment index
func (l *Log) getSegmentPath(index int) string {
	dir := filepath.Dir(l.path)
	filename := fmt.Sprintf("%018d.log", index)
	return filepath.Join(dir, filename)
}

// getGlobalOffset converts local offset to global offset
func (l *Log) getGlobalOffset(localOffset int64) int64 {
	return int64(l.segmentIndex)<<32 | localOffset
}

// parseGlobalOffset parses global offset into segment index and local offset
func (l *Log) parseGlobalOffset(globalOffset int64) (int, int64) {
	segmentIndex := int(globalOffset >> 32)
	localOffset := globalOffset & 0xFFFFFFFF
	return segmentIndex, localOffset
}

// ListSegments returns all segment files for cleanup
func (l *Log) ListSegments() ([]string, error) {
	dir := filepath.Dir(l.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) == ".log" {
			segments = append(segments, filepath.Join(dir, name))
		}
	}

	return segments, nil
}

// DeleteSegment deletes a specific segment file
func (l *Log) DeleteSegment(segmentPath string) error {
	return os.Remove(segmentPath)
}
