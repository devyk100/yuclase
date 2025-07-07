package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"yuclase/internal/network"
)

func main() {
	var (
		address = flag.String("addr", "localhost:8080", "Server address")
		command = flag.String("cmd", "", "Command to execute (if not provided, starts interactive mode)")
	)
	flag.Parse()

	// Create client
	client, err := network.NewClient(*address)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Connect to server
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Disconnect()

	if *command != "" {
		// Execute single command
		response, err := client.SendCommand(*command)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(response)
	} else {
		// Start interactive mode
		if err := client.StartInteractiveMode(); err != nil {
			log.Fatalf("Interactive mode failed: %v", err)
		}
	}
}
