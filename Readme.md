# Simple Queue Service

This project is a simple queue service implemented in Golang. It provides basic functionalities for enqueueing and dequeueing messages to and from topics, with persistence and consumer offset management.

## Features

- Enqueue messages to a topic.
- Dequeue messages from a topic with consumer offset management.
- Persistence of messages for at least 7 days.
- Lightweight and topic-wise distributed.
- Append-only log storage with index files for efficient access.

## Project Structure

- **cmd/**: Contains the main application entry point.
- **pkg/queue/**: Implements the queue logic, including topics and consumers.
- **pkg/storage/**: Manages persistence using append-only logs and index files.
- **internal/network/**: Handles TCP connections for producers and consumers.
- **internal/config/**: Manages configuration settings.
- **test/**: Contains test cases for queue and storage operations.

## Getting Started

### Prerequisites

- Go 1.16 or later

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/queue-service.git
   cd queue-service
   ```

2. Build the project:
   ```bash
   go build -o queue-service cmd/main.go
   ```

### Usage

1. Run the queue service:
   ```bash
   ./queue-service
   ```

2. Use the client to enqueue and dequeue messages.

### Testing

Run the tests using:
```bash
go test ./...
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License.
