import { Socket } from 'net';
import { EventEmitter } from 'events';

export interface YuclaseClientOptions {
  host?: string;
  port?: number;
  timeout?: number;
  reconnect?: boolean;
  reconnectDelay?: number;
  maxReconnectAttempts?: number;
}

export interface TopicStats {
  name: string;
  message_count: number;
  size: number;
  consumers: number;
}

export interface QueueStats {
  topic_count: number;
  total_messages: number;
  total_size: number;
}

export interface ConsumerOffset {
  consumer_id: string;
  offset: number;
}

export class YuclaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'YuclaseError';
  }
}

export class YuclaseClient extends EventEmitter {
  private socket: Socket | null = null;
  private connected = false;
  private connecting = false;
  private reconnectAttempts = 0;
  private pendingCommands: Map<string, { resolve: Function; reject: Function }> = new Map();
  private commandId = 0;

  private readonly options: Required<YuclaseClientOptions>;

  constructor(options: YuclaseClientOptions = {}) {
    super();
    
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 8080,
      timeout: options.timeout || 30000,
      reconnect: options.reconnect !== false,
      reconnectDelay: options.reconnectDelay || 1000,
      maxReconnectAttempts: options.maxReconnectAttempts || 10,
    };
  }

  /**
   * Connect to the Yuclase server
   */
  async connect(): Promise<void> {
    if (this.connected || this.connecting) {
      return;
    }

    this.connecting = true;

    return new Promise((resolve, reject) => {
      this.socket = new Socket();
      
      const timeout = setTimeout(() => {
        this.socket?.destroy();
        reject(new YuclaseError(`Connection timeout after ${this.options.timeout}ms`));
      }, this.options.timeout);

      this.socket.connect(this.options.port, this.options.host, () => {
        clearTimeout(timeout);
        this.connected = true;
        this.connecting = false;
        this.reconnectAttempts = 0;
        this.emit('connect');
        resolve();
      });

      this.socket.on('error', (error) => {
        clearTimeout(timeout);
        this.connecting = false;
        if (!this.connected) {
          reject(new YuclaseError(`Connection failed: ${error.message}`));
        } else {
          this.emit('error', error);
        }
      });

      this.socket.on('close', () => {
        this.connected = false;
        this.connecting = false;
        this.emit('disconnect');
        
        // Reject all pending commands
        for (const [id, { reject }] of this.pendingCommands) {
          reject(new YuclaseError('Connection closed'));
        }
        this.pendingCommands.clear();

        // Auto-reconnect if enabled
        if (this.options.reconnect && this.reconnectAttempts < this.options.maxReconnectAttempts) {
          this.reconnectAttempts++;
          setTimeout(() => {
            this.connect().catch(() => {
              // Reconnection failed, will try again or give up
            });
          }, this.options.reconnectDelay);
        }
      });

      this.socket.on('data', (data) => {
        this.handleResponse(data.toString());
      });
    });
  }

  /**
   * Disconnect from the server
   */
  async disconnect(): Promise<void> {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.connecting = false;
  }

  /**
   * Check if client is connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Send a command to the server
   */
  private async sendCommand(command: string): Promise<string> {
    if (!this.connected) {
      throw new YuclaseError('Not connected to server');
    }

    const id = (++this.commandId).toString();
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingCommands.delete(id);
        reject(new YuclaseError(`Command timeout: ${command}`));
      }, this.options.timeout);

      this.pendingCommands.set(id, {
        resolve: (response: string) => {
          clearTimeout(timeout);
          resolve(response);
        },
        reject: (error: Error) => {
          clearTimeout(timeout);
          reject(error);
        }
      });

      this.socket?.write(`${command}\r\n`);
    });
  }

  /**
   * Handle server response
   */
  private handleResponse(data: string): void {
    const lines = data.trim().split('\r\n');
    
    for (const line of lines) {
      if (line.startsWith('ERR ')) {
        // Error response
        const error = new YuclaseError(line.substring(4));
        const firstEntry = this.pendingCommands.entries().next().value;
        if (firstEntry) {
          const [id, { reject }] = firstEntry;
          this.pendingCommands.delete(id);
          reject(error);
        }
      } else {
        // Success response
        const firstEntry = this.pendingCommands.entries().next().value;
        if (firstEntry) {
          const [id, { resolve }] = firstEntry;
          this.pendingCommands.delete(id);
          resolve(line);
        }
      }
    }
  }

  /**
   * Parse array response from server
   */
  private parseArrayResponse(response: string): string[] {
    if (response === '(empty array)') {
      return [];
    }
    
    const lines = response.split('\n');
    const result: string[] = [];
    
    for (let i = 0; i < lines.length; i += 2) {
      if (lines[i + 1]) {
        result.push(lines[i + 1]);
      }
    }
    
    return result;
  }

  /**
   * Parse object response from server
   */
  private parseObjectResponse(response: string): Record<string, any> {
    const lines = response.split('\n');
    const result: Record<string, any> = {};
    
    for (let i = 0; i < lines.length; i += 2) {
      if (lines[i + 1]) {
        const key = lines[i + 1];
        const value = lines[i + 2];
        
        // Try to parse as number
        if (value && !isNaN(Number(value))) {
          result[key] = Number(value);
        } else {
          result[key] = value;
        }
        i++; // Skip the value line
      }
    }
    
    return result;
  }

  // Public API methods

  /**
   * Ping the server
   */
  async ping(message?: string): Promise<string> {
    const command = message ? `PING ${message}` : 'PING';
    return await this.sendCommand(command);
  }

  /**
   * Create a new topic
   */
  async createTopic(topic: string): Promise<void> {
    const response = await this.sendCommand(`CREATE ${topic}`);
    if (response !== 'OK') {
      throw new YuclaseError(`Failed to create topic: ${response}`);
    }
  }

  /**
   * Delete a topic
   */
  async deleteTopic(topic: string): Promise<void> {
    const response = await this.sendCommand(`DELETE ${topic}`);
    if (response !== 'OK') {
      throw new YuclaseError(`Failed to delete topic: ${response}`);
    }
  }

  /**
   * Enqueue a message to a topic
   */
  async enqueue(topic: string, message: string): Promise<number> {
    const response = await this.sendCommand(`ENQUEUE ${topic} ${message}`);
    const offset = parseInt(response);
    if (isNaN(offset)) {
      throw new YuclaseError(`Invalid offset response: ${response}`);
    }
    return offset;
  }

  /**
   * Listen for messages from a topic
   */
  async listen(topic: string, consumerId: string): Promise<string[]> {
    const response = await this.sendCommand(`LISTEN ${topic} ${consumerId}`);
    return this.parseArrayResponse(response);
  }

  /**
   * Get consumer offset
   */
  async getOffset(topic: string, consumerId: string): Promise<number> {
    const response = await this.sendCommand(`OFFSET ${topic} ${consumerId}`);
    const offset = parseInt(response);
    if (isNaN(offset)) {
      throw new YuclaseError(`Invalid offset response: ${response}`);
    }
    return offset;
  }

  /**
   * Set consumer offset
   */
  async setOffset(topic: string, consumerId: string, offset: number): Promise<void> {
    const response = await this.sendCommand(`OFFSET ${topic} ${consumerId} ${offset}`);
    if (response !== 'OK') {
      throw new YuclaseError(`Failed to set offset: ${response}`);
    }
  }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> {
    const response = await this.sendCommand('TOPICS');
    return this.parseArrayResponse(response);
  }

  /**
   * Get topic statistics
   */
  async getTopicStats(topic: string): Promise<TopicStats> {
    const response = await this.sendCommand(`STATS ${topic}`);
    const stats = this.parseObjectResponse(response);
    return {
      name: stats.name,
      message_count: stats.message_count,
      size: stats.size,
      consumers: stats.consumers,
    };
  }

  /**
   * Get queue statistics
   */
  async getQueueStats(): Promise<QueueStats> {
    const response = await this.sendCommand('STATS');
    const stats = this.parseObjectResponse(response);
    return {
      topic_count: stats.topic_count,
      total_messages: stats.total_messages,
      total_size: stats.total_size,
    };
  }

  /**
   * List consumers for a topic
   */
  async listConsumers(topic: string): Promise<string[]> {
    const response = await this.sendCommand(`CONSUMERS ${topic}`);
    return this.parseArrayResponse(response);
  }
}

// Export default instance for convenience
export default YuclaseClient;
