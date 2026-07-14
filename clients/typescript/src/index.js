const { Socket } = require('net');
const { EventEmitter } = require('events');

class YuclaseError extends Error {
  constructor(message) {
    super(message);
    this.name = 'YuclaseError';
  }
}

class YuclaseClient extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 8080,
      timeout: options.timeout || 30000,
      reconnect: options.reconnect !== false,
      reconnectDelay: options.reconnectDelay || 1000,
      maxReconnectAttempts: options.maxReconnectAttempts || 10,
    };

    this.socket = null;
    this.connected = false;
    this.connecting = false;
    this.reconnectAttempts = 0;
    this.pendingCommands = new Map();
    this.commandId = 0;
  }

  async connect() {
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

  async disconnect() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.connecting = false;
  }

  isConnected() {
    return this.connected;
  }

  async sendCommand(command) {
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
        resolve: (response) => {
          clearTimeout(timeout);
          resolve(response);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        }
      });

      this.socket?.write(`${command}\r\n`);
    });
  }

  handleResponse(data) {
    // Parse RESP protocol response
    const response = this.parseRESP(data.trim());
    
    const firstEntry = this.pendingCommands.entries().next().value;
    if (firstEntry) {
      const [id, { resolve, reject }] = firstEntry;
      this.pendingCommands.delete(id);
      
      if (response instanceof Error) {
        reject(response);
      } else {
        resolve(response);
      }
    }
  }

  parseRESP(data) {
    const lines = data.split('\r\n');
    if (lines.length === 0) return '';
    
    const firstLine = lines[0];
    
    if (firstLine.startsWith('-')) {
      // Error
      return new YuclaseError(firstLine.substring(1));
    } else if (firstLine.startsWith('+')) {
      // Simple string
      return firstLine.substring(1);
    } else if (firstLine.startsWith(':')) {
      // Integer
      return firstLine.substring(1);
    } else if (firstLine.startsWith('$')) {
      // Bulk string
      const length = parseInt(firstLine.substring(1));
      if (length === -1) return null;
      if (length === 0) return '';
      return lines[1] || '';
    } else if (firstLine.startsWith('*')) {
      // Array
      const count = parseInt(firstLine.substring(1));
      if (count === -1) return null;
      if (count === 0) return [];
      
      // Return the full response for array parsing
      return data;
    }
    
    // Fallback - return as is
    return firstLine;
  }

  parseArrayResponse(response) {
    if (response === '(empty array)') {
      return [];
    }
    
    const lines = response.split('\n');
    const result = [];
    
    for (let i = 0; i < lines.length; i += 2) {
      if (lines[i + 1]) {
        result.push(lines[i + 1]);
      }
    }
    
    return result;
  }

  parseObjectResponse(response) {
    const lines = response.split('\n');
    const result = {};
    
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

  async ping(message) {
    const command = message ? `PING ${message}` : 'PING';
    return await this.sendCommand(command);
  }

  async createTopic(topic) {
    const response = await this.sendCommand(`CREATE ${topic}`);
    if (response !== 'OK' && response !== '+OK') {
      throw new YuclaseError(`Failed to create topic: ${response}`);
    }
  }

  async deleteTopic(topic) {
    const response = await this.sendCommand(`DELETE ${topic}`);
    if (response !== 'OK' && response !== '+OK') {
      throw new YuclaseError(`Failed to delete topic: ${response}`);
    }
  }

  async enqueue(topic, message) {
    const response = await this.sendCommand(`ENQUEUE ${topic} ${message}`);
    const offset = parseInt(response);
    if (isNaN(offset)) {
      throw new YuclaseError(`Invalid offset response: ${response}`);
    }
    return offset;
  }

  async listen(topic, consumerId) {
    const response = await this.sendCommand(`LISTEN ${topic} ${consumerId}`);
    return this.parseArrayResponse(response);
  }

  async getOffset(topic, consumerId) {
    const response = await this.sendCommand(`OFFSET ${topic} ${consumerId}`);
    const offset = parseInt(response);
    if (isNaN(offset)) {
      throw new YuclaseError(`Invalid offset response: ${response}`);
    }
    return offset;
  }

  async setOffset(topic, consumerId, offset) {
    const response = await this.sendCommand(`OFFSET ${topic} ${consumerId} ${offset}`);
    if (response !== 'OK') {
      throw new YuclaseError(`Failed to set offset: ${response}`);
    }
  }

  async listTopics() {
    const response = await this.sendCommand('TOPICS');
    return this.parseArrayResponse(response);
  }

  async getTopicStats(topic) {
    const response = await this.sendCommand(`STATS ${topic}`);
    const stats = this.parseObjectResponse(response);
    return {
      name: stats.name,
      message_count: stats.message_count,
      size: stats.size,
      consumers: stats.consumers,
    };
  }

  async getQueueStats() {
    const response = await this.sendCommand('STATS');
    const stats = this.parseObjectResponse(response);
    return {
      topic_count: stats.topic_count,
      total_messages: stats.total_messages,
      total_size: stats.total_size,
    };
  }

  async listConsumers(topic) {
    const response = await this.sendCommand(`CONSUMERS ${topic}`);
    return this.parseArrayResponse(response);
  }
}

module.exports = {
  YuclaseClient,
  YuclaseError,
};
