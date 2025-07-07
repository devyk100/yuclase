# Yuclase - High-Performance Queue Service

Yuclase is a high-performance, persistent message queue service built in Go with a focus on simplicity, reliability, and performance. It implements an append-only log storage engine with consumer offset tracking and provides both a server and CLI client for easy interaction.

## Features

- **Append-Only Log Storage**: Fast, durable message storage using append-only logs
- **Topic-Based Messaging**: Organize messages into topics for better organization
- **Consumer Offset Tracking**: Each consumer maintains its own offset for independent message consumption
- **Persistent Storage**: Messages are persisted to disk with configurable retention
- **Socket-Based Communication**: TCP socket server with Redis-like protocol
- **CLI Client**: Interactive and command-line interface for easy interaction
- **Concurrent Safe**: Built with Go's concurrency primitives for thread-safe operations
- **Google Project Structure**: Clean, maintainable codebase following Go best practices

## Architecture

### Storage Engine
- **Append-Only Log**: Messages are written sequentially to log files for maximum write performance
- **Segmented Storage**: Log files are segmented for efficient cleanup and management
- **Index Files**: Optional indexing for faster random access to messages
- **Consumer Catalog**: JSON-based storage for consumer offset tracking

### Network Protocol
- **Redis-Like Protocol**: Uses RESP (Redis Serialization Protocol) for communication
- **TCP Socket Server**: Handles multiple concurrent client connections
- **Command-Based Interface**: Simple text-based commands for all operations

## Quick Start

### Build the Project

```bash
make build
```

This creates two binaries:
- `./bin/yuclase` - The queue server
- `./bin/yuclase-cli` - The CLI client

### Start the Server

```bash
./bin/yuclase
```

The server will start listening on `localhost:8080` by default.

### Run the Demo

```bash
./scripts/demo.sh
```

This demonstrates all the key features of Yuclase.

### Interactive CLI

```bash
./bin/yuclase-cli
```

This starts an interactive session where you can run commands.

## Usage Examples

### Basic Operations

```bash
# Create a topic
./bin/yuclase-cli -cmd "CREATE my-topic"

# Enqueue messages
./bin/yuclase-cli -cmd "ENQUEUE my-topic Hello World"
./bin/yuclase-cli -cmd "ENQUEUE my-topic Another message"

# Listen for messages (consumer1)
./bin/yuclase-cli -cmd "LISTEN my-topic consumer1"

# Check consumer offset
./bin/yuclase-cli -cmd "OFFSET my-topic consumer1"

# Reset consumer offset
./bin/yuclase-cli -cmd "OFFSET my-topic consumer1 0"

# List all topics
./bin/yuclase-cli -cmd "TOPICS"

# Get topic statistics
./bin/yuclase-cli -cmd "STATS my-topic"

# List consumers for a topic
./bin/yuclase-cli -cmd "CONSUMERS my-topic"
```

### Interactive Mode

```bash
$ ./bin/yuclase-cli
Yuclase CLI - Type 'help' for available commands, 'quit' to exit
yuclase> help
1) ENQUEUE <topic> <message> - Add message to topic
2) LISTEN <topic> <consumer_id> - Get messages for consumer
3) OFFSET <topic> <consumer_id> [offset] - Get/set consumer offset
4) TOPICS - List all topics
5) STATS [topic] - Get queue or topic statistics
6) CONSUMERS <topic> - List consumers for topic
7) CREATE <topic> - Create a new topic
8) DELETE <topic> - Delete a topic
9) PING [message] - Ping the server
10) QUIT - Disconnect from server
11) HELP - Show this help

yuclase> create test-topic
OK
yuclase> enqueue test-topic Hello from interactive mode
0
yuclase> listen test-topic consumer1
1) Hello from interactive mode
yuclase> quit
```

## Available Commands

| Command | Description | Example |
|---------|-------------|---------|
| `CREATE <topic>` | Create a new topic | `CREATE my-topic` |
| `DELETE <topic>` | Delete a topic | `DELETE my-topic` |
| `ENQUEUE <topic> <message>` | Add message to topic | `ENQUEUE my-topic Hello World` |
| `LISTEN <topic> <consumer_id>` | Get messages for consumer | `LISTEN my-topic consumer1` |
| `OFFSET <topic> <consumer_id> [offset]` | Get/set consumer offset | `OFFSET my-topic consumer1 0` |
| `TOPICS` | List all topics | `TOPICS` |
| `STATS [topic]` | Get statistics | `STATS my-topic` |
| `CONSUMERS <topic>` | List consumers for topic | `CONSUMERS my-topic` |
| `PING [message]` | Ping the server | `PING` |
| `HELP` | Show help | `HELP` |
| `QUIT` | Disconnect from server | `QUIT` |

## Configuration

The server can be configured using the `config.yaml` file:

```yaml
server:
  host: "localhost"
  port: 8080

storage:
  data_directory: "./data"
  log_segment_size: 1048576  # 1MB
  index_interval: 1000
  sync_interval: "1s"
  retention_duration: "168h"  # 7 days

logging:
  level: "info"
  file: "./logs/yuclase.log"
```

## Project Structure

```
yuclase/
├── cmd/                    # Application entry points
│   ├── main.go            # Server main
│   └── cli/
│       └── main.go        # CLI client main
├── internal/              # Private application code
│   ├── config/            # Configuration management
│   └── network/           # Network layer (server/client)
├── pkg/                   # Public library code
│   ├── queue/             # Queue management
│   └── storage/           # Storage engine
├── test/                  # Test files
├── scripts/               # Build and demo scripts
├── data/                  # Data directory (created at runtime)
├── logs/                  # Log directory (created at runtime)
├── config.yaml           # Configuration file
├── Makefile              # Build automation
└── Readme.md             # This file
```

## Storage Format

### Log File Format
Each message in the log file follows this format:
```
[timestamp:8 bytes][size:4 bytes][data:size bytes]
```

- **Timestamp**: 8-byte big-endian Unix nanosecond timestamp
- **Size**: 4-byte big-endian message size
- **Data**: Variable-length message data

### Directory Structure
```
data/
└── topics/
    └── <topic-name>/
        ├── 000000000000000000.log  # Log segment files
        ├── 000000000000000001.log
        ├── index.idx               # Index file
        └── consumers.json          # Consumer offset catalog
```

## Performance Characteristics

- **Write Performance**: Optimized for high-throughput sequential writes
- **Read Performance**: Efficient sequential reads with optional indexing
- **Memory Usage**: Minimal memory footprint with disk-based storage
- **Concurrency**: Thread-safe operations using Go's sync primitives
- **Durability**: Configurable sync intervals for durability vs performance trade-offs

## Development

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd yuclase

# Build the project
make build

# Run tests
make test

# Clean build artifacts
make clean
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test ./pkg/storage
go test ./pkg/queue
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by Apache Kafka's log-based architecture
- Uses Redis-like protocol for simplicity
- Built with Go's excellent concurrency primitives
