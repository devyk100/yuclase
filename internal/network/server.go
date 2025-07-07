package network

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"yuclase/internal/config"
	"yuclase/pkg/queue"
)

type Server struct {
	mu       sync.RWMutex
	config   *config.Config
	queue    *queue.Queue
	listener net.Listener
	clients  map[string]*ClientConnection
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type ClientConnection struct {
	conn     net.Conn
	id       string
	scanner  *bufio.Scanner
	writer   *bufio.Writer
	lastSeen time.Time
	active   bool
}

type Command struct {
	Name string
	Args []string
}

// NewServer creates a new server instance
func NewServer(config *config.Config) (*Server, error) {
	// Create queue instance
	q, err := queue.NewQueue(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config:  config,
		queue:   q,
		clients: make(map[string]*ClientConnection),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start starts the server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.GetServerAddress())
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.GetServerAddress(), err)
	}

	s.listener = listener
	fmt.Printf("Yuclase queue server listening on %s\n", s.config.GetServerAddress())

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	// Wait for context cancellation
	<-s.ctx.Done()

	return nil
}

// Stop stops the server gracefully
func (s *Server) Stop() error {
	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all client connections
	s.mu.Lock()
	for _, client := range s.clients {
		client.conn.Close()
	}
	s.mu.Unlock()

	// Wait for all goroutines to finish
	s.wg.Wait()

	// Close queue
	if s.queue != nil {
		return s.queue.Close()
	}

	return nil
}

// acceptConnections accepts incoming client connections
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				continue
			}
		}

		// Handle connection in a new goroutine
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())

	client := &ClientConnection{
		conn:     conn,
		id:       clientID,
		scanner:  bufio.NewScanner(conn),
		writer:   bufio.NewWriter(conn),
		lastSeen: time.Now(),
		active:   true,
	}

	// Register client
	s.mu.Lock()
	s.clients[clientID] = client
	s.mu.Unlock()

	// Remove client when done
	defer func() {
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
	}()

	// Send welcome message
	s.sendResponse(client, "+OK Yuclase Queue Server Ready\r\n")

	// Handle commands
	for client.scanner.Scan() {
		line := strings.TrimSpace(client.scanner.Text())
		if line == "" {
			continue
		}

		client.lastSeen = time.Now()

		// Parse command
		cmd, err := s.parseCommand(line)
		if err != nil {
			s.sendError(client, fmt.Sprintf("ERR %s", err.Error()))
			continue
		}

		// Execute command
		if err := s.executeCommand(client, cmd); err != nil {
			s.sendError(client, fmt.Sprintf("ERR %s", err.Error()))
		}
	}
}

// parseCommand parses a command line into a Command struct
func (s *Server) parseCommand(line string) (*Command, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}

// executeCommand executes a parsed command
func (s *Server) executeCommand(client *ClientConnection, cmd *Command) error {
	switch cmd.Name {
	case "ENQUEUE":
		return s.handleEnqueue(client, cmd.Args)
	case "LISTEN":
		return s.handleListen(client, cmd.Args)
	case "OFFSET":
		return s.handleOffset(client, cmd.Args)
	case "TOPICS":
		return s.handleTopics(client, cmd.Args)
	case "STATS":
		return s.handleStats(client, cmd.Args)
	case "CONSUMERS":
		return s.handleConsumers(client, cmd.Args)
	case "CREATE":
		return s.handleCreate(client, cmd.Args)
	case "DELETE":
		return s.handleDelete(client, cmd.Args)
	case "PING":
		return s.handlePing(client, cmd.Args)
	case "QUIT":
		return s.handleQuit(client, cmd.Args)
	case "HELP":
		return s.handleHelp(client, cmd.Args)
	default:
		return fmt.Errorf("unknown command: %s", cmd.Name)
	}
}

// handleEnqueue handles ENQUEUE command
func (s *Server) handleEnqueue(client *ClientConnection, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("ENQUEUE requires topic and message")
	}

	topicName := args[0]
	message := strings.Join(args[1:], " ")

	offset, err := s.queue.Enqueue(topicName, []byte(message))
	if err != nil {
		return fmt.Errorf("failed to enqueue: %w", err)
	}

	return s.sendResponse(client, fmt.Sprintf(":%d\r\n", offset))
}

// handleListen handles LISTEN command
func (s *Server) handleListen(client *ClientConnection, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("LISTEN requires topic and consumer_id")
	}

	topicName := args[0]
	consumerID := args[1]

	// Check if topic exists
	if !s.queue.TopicExists(topicName) {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	// Start listening for messages
	messages, err := s.queue.Dequeue(topicName, consumerID)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}

	// Send messages
	if len(messages) == 0 {
		return s.sendResponse(client, "*0\r\n")
	}

	response := fmt.Sprintf("*%d\r\n", len(messages))
	for _, msg := range messages {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(msg.Data), string(msg.Data))
	}

	return s.sendResponse(client, response)
}

// handleOffset handles OFFSET command
func (s *Server) handleOffset(client *ClientConnection, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("OFFSET requires topic and consumer_id")
	}

	topicName := args[0]
	consumerID := args[1]

	if len(args) == 3 {
		// Set offset
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset: %s", args[2])
		}

		if err := s.queue.UpdateConsumerOffset(topicName, consumerID, offset); err != nil {
			return fmt.Errorf("failed to update offset: %w", err)
		}

		return s.sendResponse(client, "+OK\r\n")
	} else {
		// Get offset
		offset, err := s.queue.GetConsumerOffset(topicName, consumerID)
		if err != nil {
			return fmt.Errorf("failed to get offset: %w", err)
		}

		return s.sendResponse(client, fmt.Sprintf(":%d\r\n", offset))
	}
}

// handleTopics handles TOPICS command
func (s *Server) handleTopics(client *ClientConnection, args []string) error {
	topics := s.queue.ListTopics()

	response := fmt.Sprintf("*%d\r\n", len(topics))
	for _, topic := range topics {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(topic), topic)
	}

	return s.sendResponse(client, response)
}

// handleStats handles STATS command
func (s *Server) handleStats(client *ClientConnection, args []string) error {
	if len(args) == 0 {
		// Queue stats
		stats, err := s.queue.GetQueueStats()
		if err != nil {
			return fmt.Errorf("failed to get queue stats: %w", err)
		}

		response := fmt.Sprintf("*6\r\n")
		response += fmt.Sprintf("$11\r\ntopic_count\r\n:%d\r\n", stats.TopicCount)
		response += fmt.Sprintf("$14\r\ntotal_messages\r\n:%d\r\n", stats.TotalMessages)
		response += fmt.Sprintf("$10\r\ntotal_size\r\n:%d\r\n", stats.TotalSize)

		return s.sendResponse(client, response)
	} else {
		// Topic stats
		topicName := args[0]
		stats, err := s.queue.GetTopicStats(topicName)
		if err != nil {
			return fmt.Errorf("failed to get topic stats: %w", err)
		}

		response := fmt.Sprintf("*8\r\n")
		response += fmt.Sprintf("$4\r\nname\r\n$%d\r\n%s\r\n", len(stats.Name), stats.Name)
		response += fmt.Sprintf("$13\r\nmessage_count\r\n:%d\r\n", stats.MessageCount)
		response += fmt.Sprintf("$4\r\nsize\r\n:%d\r\n", stats.Size)
		response += fmt.Sprintf("$9\r\nconsumers\r\n:%d\r\n", stats.Consumers)

		return s.sendResponse(client, response)
	}
}

// handleConsumers handles CONSUMERS command
func (s *Server) handleConsumers(client *ClientConnection, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("CONSUMERS requires topic")
	}

	topicName := args[0]
	consumers, err := s.queue.ListConsumers(topicName)
	if err != nil {
		return fmt.Errorf("failed to list consumers: %w", err)
	}

	response := fmt.Sprintf("*%d\r\n", len(consumers))
	for _, consumer := range consumers {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(consumer), consumer)
	}

	return s.sendResponse(client, response)
}

// handleCreate handles CREATE command
func (s *Server) handleCreate(client *ClientConnection, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("CREATE requires topic name")
	}

	topicName := args[0]
	if err := s.queue.CreateTopic(topicName); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return s.sendResponse(client, "+OK\r\n")
}

// handleDelete handles DELETE command
func (s *Server) handleDelete(client *ClientConnection, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("DELETE requires topic name")
	}

	topicName := args[0]
	if err := s.queue.DeleteTopic(topicName); err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	return s.sendResponse(client, "+OK\r\n")
}

// handlePing handles PING command
func (s *Server) handlePing(client *ClientConnection, args []string) error {
	if len(args) == 0 {
		return s.sendResponse(client, "+PONG\r\n")
	}
	return s.sendResponse(client, fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0]))
}

// handleQuit handles QUIT command
func (s *Server) handleQuit(client *ClientConnection, args []string) error {
	s.sendResponse(client, "+OK\r\n")
	client.active = false
	return fmt.Errorf("client disconnected")
}

// handleHelp handles HELP command
func (s *Server) handleHelp(client *ClientConnection, args []string) error {
	help := []string{
		"ENQUEUE <topic> <message> - Add message to topic",
		"LISTEN <topic> <consumer_id> - Get messages for consumer",
		"OFFSET <topic> <consumer_id> [offset] - Get/set consumer offset",
		"TOPICS - List all topics",
		"STATS [topic] - Get queue or topic statistics",
		"CONSUMERS <topic> - List consumers for topic",
		"CREATE <topic> - Create a new topic",
		"DELETE <topic> - Delete a topic",
		"PING [message] - Ping the server",
		"QUIT - Disconnect from server",
		"HELP - Show this help",
	}

	response := fmt.Sprintf("*%d\r\n", len(help))
	for _, line := range help {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(line), line)
	}

	return s.sendResponse(client, response)
}

// sendResponse sends a response to the client
func (s *Server) sendResponse(client *ClientConnection, response string) error {
	_, err := client.writer.WriteString(response)
	if err != nil {
		return err
	}
	return client.writer.Flush()
}

// sendError sends an error response to the client
func (s *Server) sendError(client *ClientConnection, message string) error {
	return s.sendResponse(client, fmt.Sprintf("-%s\r\n", message))
}

// GetClientCount returns the number of connected clients
func (s *Server) GetClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// GetServerStats returns server statistics
func (s *Server) GetServerStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"client_count": len(s.clients),
		"uptime":       time.Since(time.Now()).String(), // This would be calculated properly
		"address":      s.config.GetServerAddress(),
	}
}
