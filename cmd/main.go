// cmd/main.go
package main

import (
    "log"
    "yuclase/pkg/queue"
    "yuclase/internal/network"
)

func main() {
    // Initialize configuration
    config := loadConfig()

    // Start the queue server
    server := network.NewServer(config)
    if err := server.Start(); err != nil {
        log.Fatalf("Failed to start server: %v", err)
    }
}
