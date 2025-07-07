package network

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type Client struct {
	conn    net.Conn
	scanner *bufio.Scanner
	writer  *bufio.Writer
	address string
}

// NewClient creates a new client instance
func NewClient(address string) (*Client, error) {
	return &Client{
		address: address,
	}, nil
}

// Connect connects the client to the server
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.address, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.address, err)
	}

	c.conn = conn
	c.scanner = bufio.NewScanner(conn)
	c.writer = bufio.NewWriter(conn)

	// Set a read timeout for the welcome message
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read welcome message
	if c.scanner.Scan() {
		response := c.scanner.Text()
		// Clear the read deadline
		c.conn.SetReadDeadline(time.Time{})

		if strings.HasPrefix(response, "+OK") {
			return nil
		}
		return fmt.Errorf("unexpected welcome message: %s", response)
	}

	// Clear the read deadline
	c.conn.SetReadDeadline(time.Time{})

	if err := c.scanner.Err(); err != nil {
		return fmt.Errorf("failed to read welcome message: %w", err)
	}

	return fmt.Errorf("failed to read welcome message")
}

// Disconnect disconnects from the server
func (c *Client) Disconnect() error {
	if c.conn != nil {
		// Send QUIT command
		c.SendCommand("QUIT")
		return c.conn.Close()
	}
	return nil
}

// SendCommand sends a command to the server and returns the response
func (c *Client) SendCommand(command string) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("not connected to server")
	}

	// Send command
	_, err := c.writer.WriteString(command + "\r\n")
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush command: %w", err)
	}

	// Read response
	return c.readResponse()
}

// readResponse reads and parses the server response
func (c *Client) readResponse() (string, error) {
	if !c.scanner.Scan() {
		return "", fmt.Errorf("failed to read response")
	}

	line := c.scanner.Text()

	switch line[0] {
	case '+': // Simple string
		return line[1:], nil
	case '-': // Error
		return "", fmt.Errorf("server error: %s", line[1:])
	case ':': // Integer
		return line[1:], nil
	case '$': // Bulk string
		return c.readBulkString(line)
	case '*': // Array
		return c.readArray(line)
	default:
		return line, nil
	}
}

// readBulkString reads a bulk string response
func (c *Client) readBulkString(header string) (string, error) {
	// Parse length
	var length int
	if _, err := fmt.Sscanf(header, "$%d", &length); err != nil {
		return "", fmt.Errorf("invalid bulk string header: %s", header)
	}

	if length == -1 {
		return "(nil)", nil
	}

	// Read the string
	if !c.scanner.Scan() {
		return "", fmt.Errorf("failed to read bulk string content")
	}

	return c.scanner.Text(), nil
}

// readArray reads an array response
func (c *Client) readArray(header string) (string, error) {
	// Parse array length
	var length int
	if _, err := fmt.Sscanf(header, "*%d", &length); err != nil {
		return "", fmt.Errorf("invalid array header: %s", header)
	}

	if length == 0 {
		return "(empty array)", nil
	}

	if length == -1 {
		return "(nil)", nil
	}

	var result strings.Builder
	for i := 0; i < length; i++ {
		if !c.scanner.Scan() {
			return "", fmt.Errorf("failed to read array element %d", i)
		}

		line := c.scanner.Text()
		switch line[0] {
		case '$':
			str, err := c.readBulkString(line)
			if err != nil {
				return "", err
			}
			result.WriteString(fmt.Sprintf("%d) %s\n", i+1, str))
		case ':':
			result.WriteString(fmt.Sprintf("%d) %s\n", i+1, line[1:]))
		default:
			result.WriteString(fmt.Sprintf("%d) %s\n", i+1, line))
		}
	}

	return result.String(), nil
}

// StartInteractiveMode starts the interactive CLI mode
func (c *Client) StartInteractiveMode() error {
	fmt.Println("Yuclase CLI - Type 'help' for available commands, 'quit' to exit")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("yuclase> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read input: %w", err)
		}

		command := strings.TrimSpace(input)
		if command == "" {
			continue
		}

		if strings.ToLower(command) == "quit" || strings.ToLower(command) == "exit" {
			break
		}

		response, err := c.SendCommand(command)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Println(response)
	}

	return nil
}

// Enqueue sends an ENQUEUE command
func (c *Client) Enqueue(topic, message string) (string, error) {
	command := fmt.Sprintf("ENQUEUE %s %s", topic, message)
	return c.SendCommand(command)
}

// Listen sends a LISTEN command
func (c *Client) Listen(topic, consumerID string) (string, error) {
	command := fmt.Sprintf("LISTEN %s %s", topic, consumerID)
	return c.SendCommand(command)
}

// GetOffset sends an OFFSET command to get consumer offset
func (c *Client) GetOffset(topic, consumerID string) (string, error) {
	command := fmt.Sprintf("OFFSET %s %s", topic, consumerID)
	return c.SendCommand(command)
}

// SetOffset sends an OFFSET command to set consumer offset
func (c *Client) SetOffset(topic, consumerID string, offset int64) (string, error) {
	command := fmt.Sprintf("OFFSET %s %s %d", topic, consumerID, offset)
	return c.SendCommand(command)
}

// ListTopics sends a TOPICS command
func (c *Client) ListTopics() (string, error) {
	return c.SendCommand("TOPICS")
}

// GetStats sends a STATS command
func (c *Client) GetStats(topic string) (string, error) {
	if topic == "" {
		return c.SendCommand("STATS")
	}
	return c.SendCommand(fmt.Sprintf("STATS %s", topic))
}

// ListConsumers sends a CONSUMERS command
func (c *Client) ListConsumers(topic string) (string, error) {
	command := fmt.Sprintf("CONSUMERS %s", topic)
	return c.SendCommand(command)
}

// CreateTopic sends a CREATE command
func (c *Client) CreateTopic(topic string) (string, error) {
	command := fmt.Sprintf("CREATE %s", topic)
	return c.SendCommand(command)
}

// DeleteTopic sends a DELETE command
func (c *Client) DeleteTopic(topic string) (string, error) {
	command := fmt.Sprintf("DELETE %s", topic)
	return c.SendCommand(command)
}

// Ping sends a PING command
func (c *Client) Ping(message string) (string, error) {
	if message == "" {
		return c.SendCommand("PING")
	}
	return c.SendCommand(fmt.Sprintf("PING %s", message))
}

// Help sends a HELP command
func (c *Client) Help() (string, error) {
	return c.SendCommand("HELP")
}
