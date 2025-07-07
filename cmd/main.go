// cmd/main.go
package main

import (
	"log"
	"yuclase/internal/config"
	"yuclase/internal/network"
)

func main() {
	// Initialize configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Start the queue server
	server, err := network.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
