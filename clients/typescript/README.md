# Yuclase TypeScript Client

A TypeScript/JavaScript client library for connecting to Yuclase queue service from any machine.

## Installation

```bash
npm install yuclase-client
```

## Quick Start

```typescript
import { YuclaseClient } from 'yuclase-client';

async function example() {
  const client = new YuclaseClient({
    host: 'your-yuclase-server.com', // or IP address
    port: 8080,
  });

  await client.connect();
  
  // Create a topic
  await client.createTopic('my-topic');
  
  // Enqueue a message
  const offset = await client.enqueue('my-topic', 'Hello World!');
  console.log(`Message enqueued at offset: ${offset}`);
  
  // Listen for messages
  const messages = await client.listen('my-topic', 'consumer1');
  console.log('Received messages:', messages);
  
  await client.disconnect();
}
```

## API Reference

### Constructor Options

```typescript
interface YuclaseClientOptions {
  host?: string;                    // Default: 'localhost'
  port?: number;                    // Default: 8080
  timeout?: number;                 // Default: 30000 (30 seconds)
  reconnect?: boolean;              // Default: true
  reconnectDelay?: number;          // Default: 1000 (1 second)
  maxReconnectAttempts?: number;    // Default: 10
}
```

### Methods

#### Connection Management

- `connect(): Promise<void>` - Connect to the Yuclase server
- `disconnect(): Promise<void>` - Disconnect from the server
- `isConnected(): boolean` - Check if client is connected
- `ping(message?: string): Promise<string>` - Ping the server

#### Topic Management

- `createTopic(topic: string): Promise<void>` - Create a new topic
- `deleteTopic(topic: string): Promise<void>` - Delete a topic
- `listTopics(): Promise<string[]>` - List all topics

#### Message Operations

- `enqueue(topic: string, message: string): Promise<number>` - Add message to topic
- `listen(topic: string, consumerId: string): Promise<string[]>` - Get messages for consumer

#### Consumer Management

- `getOffset(topic: string, consumerId: string): Promise<number>` - Get consumer offset
- `setOffset(topic: string, consumerId: string, offset: number): Promise<void>` - Set consumer offset
- `listConsumers(topic: string): Promise<string[]>` - List consumers for topic

#### Statistics

- `getTopicStats(topic: string): Promise<TopicStats>` - Get topic statistics
- `getQueueStats(): Promise<QueueStats>` - Get overall queue statistics

### Events

The client extends EventEmitter and emits the following events:

- `connect` - Emitted when connected to server
- `disconnect` - Emitted when disconnected from server
- `error` - Emitted on connection errors

```typescript
client.on('connect', () => {
  console.log('Connected to Yuclase server');
});

client.on('disconnect', () => {
  console.log('Disconnected from server');
});

client.on('error', (error) => {
  console.error('Connection error:', error);
});
```

## Examples

### Basic Usage

```typescript
import { YuclaseClient } from 'yuclase-client';

const client = new YuclaseClient({
  host: '192.168.1.100', // Your Yuclase server IP
  port: 8080,
});

await client.connect();

// Create topic and send messages
await client.createTopic('user-events');
await client.enqueue('user-events', JSON.stringify({
  userId: 123,
  action: 'login',
  timestamp: new Date().toISOString()
}));

// Process messages
const messages = await client.listen('user-events', 'processor-1');
for (const message of messages) {
  const event = JSON.parse(message);
  console.log('Processing event:', event);
}
```

### Multiple Consumers

```typescript
// Consumer 1 - processes from beginning
const consumer1Messages = await client.listen('events', 'consumer1');

// Consumer 2 - also processes from beginning (independent offset)
const consumer2Messages = await client.listen('events', 'consumer2');

// Reset consumer1 to reprocess all messages
await client.setOffset('events', 'consumer1', 0);
const reprocessedMessages = await client.listen('events', 'consumer1');
```

### Error Handling

```typescript
import { YuclaseClient, YuclaseError } from 'yuclase-client';

try {
  await client.enqueue('non-existent-topic', 'message');
} catch (error) {
  if (error instanceof YuclaseError) {
    console.error('Yuclase error:', error.message);
  } else {
    console.error('Unexpected error:', error);
  }
}
```

### Auto-Reconnection

```typescript
const client = new YuclaseClient({
  host: 'yuclase-server.com',
  reconnect: true,
  reconnectDelay: 2000,
  maxReconnectAttempts: 5,
});

client.on('disconnect', () => {
  console.log('Lost connection, will attempt to reconnect...');
});

client.on('connect', () => {
  console.log('Connected/Reconnected to server');
});
```

## Remote Connection Setup

To connect from a remote machine:

1. **Configure Yuclase Server**: Make sure your Yuclase server is configured to accept connections from other machines. Update the `config.yaml`:

```yaml
server:
  host: "0.0.0.0"  # Listen on all interfaces
  port: 8080
```

2. **Firewall**: Ensure port 8080 is open on the server machine.

3. **Client Configuration**: Use the server's IP address or hostname:

```typescript
const client = new YuclaseClient({
  host: '192.168.1.100', // Replace with your server's IP
  port: 8080,
});
```

## TypeScript Support

This library is written in TypeScript and includes full type definitions. All interfaces and types are exported:

```typescript
import { 
  YuclaseClient, 
  YuclaseClientOptions, 
  TopicStats, 
  QueueStats,
  YuclaseError 
} from 'yuclase-client';
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

- GitHub Issues: https://github.com/devyk100/yuclase/issues
- Documentation: https://github.com/devyk100/yuclase#readme
