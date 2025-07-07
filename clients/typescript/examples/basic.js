const { YuclaseClient } = require('../src/index.js');

async function basicExample() {
  // Create a client instance
  const client = new YuclaseClient({
    host: 'localhost',
    port: 8080,
    timeout: 10000,
    reconnect: true,
  });

  try {
    // Connect to the server
    console.log('Connecting to Yuclase server...');
    await client.connect();
    console.log('Connected!');

    // Test ping
    const pong = await client.ping('Hello from JavaScript!');
    console.log('Ping response:', pong);

    // Create a topic
    const topicName = 'javascript-demo';
    console.log(`Creating topic: ${topicName}`);
    await client.createTopic(topicName);

    // Enqueue some messages
    console.log('Enqueuing messages...');
    const offset1 = await client.enqueue(topicName, 'Hello from JavaScript client!');
    console.log(`Message 1 enqueued at offset: ${offset1}`);

    const offset2 = await client.enqueue(topicName, 'This is message number 2');
    console.log(`Message 2 enqueued at offset: ${offset2}`);

    const offset3 = await client.enqueue(topicName, JSON.stringify({ 
      type: 'user_action', 
      userId: 123, 
      action: 'login',
      timestamp: new Date().toISOString()
    }));
    console.log(`Message 3 (JSON) enqueued at offset: ${offset3}`);

    // List all topics
    const topics = await client.listTopics();
    console.log('Available topics:', topics);

    // Get topic statistics
    const stats = await client.getTopicStats(topicName);
    console.log('Topic statistics:', stats);

    // Listen for messages with consumer1
    console.log('\nListening for messages with consumer1...');
    const messages1 = await client.listen(topicName, 'consumer1');
    console.log('Messages received by consumer1:', messages1);

    // Check consumer offset
    const offset = await client.getOffset(topicName, 'consumer1');
    console.log(`Consumer1 offset: ${offset}`);

    // Listen again (should be empty since consumer1 already read all messages)
    console.log('\nListening again with consumer1 (should be empty)...');
    const messages2 = await client.listen(topicName, 'consumer1');
    console.log('Messages received by consumer1 (second time):', messages2);

    // Create a second consumer and listen
    console.log('\nListening with consumer2 (should get all messages)...');
    const messages3 = await client.listen(topicName, 'consumer2');
    console.log('Messages received by consumer2:', messages3);

    // Reset consumer1 offset to 0
    console.log('\nResetting consumer1 offset to 0...');
    await client.setOffset(topicName, 'consumer1', 0);

    // Listen again with consumer1
    console.log('Listening with consumer1 after reset...');
    const messages4 = await client.listen(topicName, 'consumer1');
    console.log('Messages received by consumer1 (after reset):', messages4);

    // List consumers
    const consumers = await client.listConsumers(topicName);
    console.log('Consumers for topic:', consumers);

    // Get overall queue statistics
    const queueStats = await client.getQueueStats();
    console.log('Queue statistics:', queueStats);

    console.log('\n✅ Demo completed successfully!');

  } catch (error) {
    console.error('Error:', error);
  } finally {
    // Disconnect
    await client.disconnect();
    console.log('Disconnected from server');
  }
}

// Run the example
if (require.main === module) {
  basicExample().catch(console.error);
}

module.exports = { basicExample };
