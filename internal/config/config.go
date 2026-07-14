package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Storage     StorageConfig     `yaml:"storage"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Performance PerformanceConfig `yaml:"performance"`
	Logging     LoggingConfig     `yaml:"logging"`
}

type ServerConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	MaxConnections int    `yaml:"max_connections"`
}

type StorageConfig struct {
	DataDirectory   string `yaml:"data_directory"`
	LogSegmentSize  string `yaml:"log_segment_size"`
	IndexInterval   int    `yaml:"index_interval"`
	RetentionHours  int    `yaml:"retention_hours"`
	logSegmentBytes int64  // parsed value
}

type ClusterConfig struct {
	NodeID string       `yaml:"node_id"`
	Nodes  []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID   string `yaml:"id"`
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type PerformanceConfig struct {
	WriteBufferSize  string        `yaml:"write_buffer_size"`
	ReadBufferSize   string        `yaml:"read_buffer_size"`
	SyncInterval     string        `yaml:"sync_interval"`
	CleanupInterval  string        `yaml:"cleanup_interval"`
	writeBufferBytes int           // parsed value
	readBufferBytes  int           // parsed value
	syncInterval     time.Duration // parsed value
	cleanupInterval  time.Duration // parsed value
}

type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	config := &Config{}

	// Set defaults
	config.setDefaults()

	// Load from file if exists
	if configPath != "" {
		if err := config.loadFromFile(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Override with environment variables
	config.loadFromEnv()

	// Parse and validate
	if err := config.parseAndValidate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

func (c *Config) setDefaults() {
	c.Server = ServerConfig{
		Host:           "localhost",
		Port:           8080,
		MaxConnections: 1000,
	}

	c.Storage = StorageConfig{
		DataDirectory:  "./data",
		LogSegmentSize: "100MB",
		IndexInterval:  1000,
		RetentionHours: 168, // 7 days
	}

	c.Cluster = ClusterConfig{
		NodeID: "node-1",
		Nodes: []NodeConfig{
			{ID: "node-1", Host: "localhost", Port: 8080},
		},
	}

	c.Performance = PerformanceConfig{
		WriteBufferSize: "64KB",
		ReadBufferSize:  "64KB",
		SyncInterval:    "1s",
		CleanupInterval: "1h",
	}

	c.Logging = LoggingConfig{
		Level: "info",
		File:  "./logs/yuclase.log",
	}
}

func (c *Config) loadFromFile(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, c)
}

func (c *Config) loadFromEnv() {
	if host := os.Getenv("YUCLASE_HOST"); host != "" {
		c.Server.Host = host
	}
	if port := os.Getenv("YUCLASE_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			c.Server.Port = p
		}
	}
	if dataDir := os.Getenv("YUCLASE_DATA_DIR"); dataDir != "" {
		c.Storage.DataDirectory = dataDir
	}
	if logLevel := os.Getenv("YUCLASE_LOG_LEVEL"); logLevel != "" {
		c.Logging.Level = logLevel
	}
}

func (c *Config) parseAndValidate() error {
	// Parse storage sizes
	var err error
	c.Storage.logSegmentBytes, err = parseSize(c.Storage.LogSegmentSize)
	if err != nil {
		return fmt.Errorf("invalid log_segment_size: %w", err)
	}

	writeBufferBytes, err := parseSize(c.Performance.WriteBufferSize)
	if err != nil {
		return fmt.Errorf("invalid write_buffer_size: %w", err)
	}
	c.Performance.writeBufferBytes = int(writeBufferBytes)

	readBufferBytes, err := parseSize(c.Performance.ReadBufferSize)
	if err != nil {
		return fmt.Errorf("invalid read_buffer_size: %w", err)
	}
	c.Performance.readBufferBytes = int(readBufferBytes)

	// Parse durations
	c.Performance.syncInterval, err = time.ParseDuration(c.Performance.SyncInterval)
	if err != nil {
		return fmt.Errorf("invalid sync_interval: %w", err)
	}

	c.Performance.cleanupInterval, err = time.ParseDuration(c.Performance.CleanupInterval)
	if err != nil {
		return fmt.Errorf("invalid cleanup_interval: %w", err)
	}

	// Validate values
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Server.Port)
	}

	if c.Storage.RetentionHours <= 0 {
		return fmt.Errorf("retention_hours must be positive")
	}

	if c.Storage.IndexInterval <= 0 {
		return fmt.Errorf("index_interval must be positive")
	}

	return nil
}

// parseSize parses size strings like "100MB", "64KB", etc.
func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(sizeStr, "KB")
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(sizeStr, "MB")
	} else if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(sizeStr, "GB")
	} else if strings.HasSuffix(sizeStr, "B") {
		numStr = strings.TrimSuffix(sizeStr, "B")
	} else {
		numStr = sizeStr
	}

	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, err
	}

	result := num * multiplier
	if result < 0 { // Check for overflow
		return 0, fmt.Errorf("size too large")
	}

	return result, nil
}

// Getter methods for parsed values
func (c *Config) GetLogSegmentBytes() int64 {
	return c.Storage.logSegmentBytes
}

func (c *Config) GetWriteBufferBytes() int {
	return c.Performance.writeBufferBytes
}

func (c *Config) GetReadBufferBytes() int {
	return c.Performance.readBufferBytes
}

func (c *Config) GetSyncInterval() time.Duration {
	return c.Performance.syncInterval
}

func (c *Config) GetCleanupInterval() time.Duration {
	return c.Performance.cleanupInterval
}

func (c *Config) GetRetentionDuration() time.Duration {
	return time.Duration(c.Storage.RetentionHours) * time.Hour
}

func (c *Config) GetServerAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}
