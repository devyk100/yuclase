.PHONY: build clean test run run-cli help

# Build configuration
BINARY_NAME=yuclase
CLI_BINARY_NAME=yuclase-cli
BUILD_DIR=./bin
GO_FILES=$(shell find . -name "*.go" -type f)

# Default target
all: build

# Build both server and CLI
build: build-server build-cli

# Build server
build-server:
	@echo "Building server..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) cmd/main.go

# Build CLI client
build-cli:
	@echo "Building CLI client..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(CLI_BINARY_NAME) cmd/cli/main.go

# Build for multiple platforms
build-all: build-linux build-windows build-darwin

build-linux:
	@echo "Building for Linux..."
	@mkdir -p $(BUILD_DIR)/linux
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/linux/$(BINARY_NAME) cmd/main.go
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/linux/$(CLI_BINARY_NAME) cmd/cli/main.go

build-windows:
	@echo "Building for Windows..."
	@mkdir -p $(BUILD_DIR)/windows
	GOOS=windows GOARCH=amd64 go build -o $(BUILD_DIR)/windows/$(BINARY_NAME).exe cmd/main.go
	GOOS=windows GOARCH=amd64 go build -o $(BUILD_DIR)/windows/$(CLI_BINARY_NAME).exe cmd/cli/main.go

build-darwin:
	@echo "Building for macOS..."
	@mkdir -p $(BUILD_DIR)/darwin
	GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/darwin/$(BINARY_NAME) cmd/main.go
	GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/darwin/$(CLI_BINARY_NAME) cmd/cli/main.go

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run the server
run: build-server
	@echo "Starting Yuclase server..."
	./$(BUILD_DIR)/$(BINARY_NAME)

# Run the CLI client
run-cli: build-cli
	@echo "Starting Yuclase CLI..."
	./$(BUILD_DIR)/$(CLI_BINARY_NAME)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -f $(BINARY_NAME) $(CLI_BINARY_NAME)

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run

# Create data directory
setup:
	@echo "Setting up data directory..."
	mkdir -p data/topics
	mkdir -p logs

# Development setup
dev-setup: deps setup
	@echo "Development environment ready!"

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t yuclase:latest .

# Docker run
docker-run:
	@echo "Running Docker container..."
	docker run -p 8080:8080 -v $(PWD)/data:/app/data yuclase:latest

# Help
help:
	@echo "Available targets:"
	@echo "  build         - Build both server and CLI"
	@echo "  build-server  - Build server only"
	@echo "  build-cli     - Build CLI client only"
	@echo "  build-all     - Build for all platforms"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage"
	@echo "  run           - Build and run server"
	@echo "  run-cli       - Build and run CLI client"
	@echo "  clean         - Clean build artifacts"
	@echo "  deps          - Install dependencies"
	@echo "  fmt           - Format code"
	@echo "  lint          - Lint code"
	@echo "  setup         - Create data directory"
	@echo "  dev-setup     - Setup development environment"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-run    - Run Docker container"
	@echo "  help          - Show this help"
