#!/bin/bash

# Yuclase Demo Script
# This script demonstrates the basic functionality of Yuclase

# Remove set -e to handle errors gracefully
# set -e

echo "🚀 Yuclase Queue Service Demo"
echo "=============================="

# Check if binaries exist
if [ ! -f "./bin/yuclase" ]; then
    echo "❌ Server binary not found. Run 'make build' first."
    exit 1
fi

if [ ! -f "./bin/yuclase-cli" ]; then
    echo "❌ CLI binary not found. Run 'make build' first."
    exit 1
fi

# Start the server in background
echo "📡 Starting Yuclase server..."
./bin/yuclase &
SERVER_PID=$!

# Wait for server to start and be ready
echo "⏳ Waiting for server to be ready..."
sleep 3

# Test if server is ready
for i in {1..10}; do
    if ./bin/yuclase-cli -cmd "PING" >/dev/null 2>&1; then
        break
    fi
    echo "   Waiting... ($i/10)"
    sleep 1
done

# Function to cleanup on exit
cleanup() {
    echo "🧹 Cleaning up..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "✅ Server started (PID: $SERVER_PID)"
echo ""

# Test basic connectivity
echo "🔍 Testing server connectivity..."
PING_RESULT=$(./bin/yuclase-cli -cmd "PING" 2>/dev/null || echo "FAILED")
if [ "$PING_RESULT" = "PONG" ]; then
    echo "✅ Server is responding"
else
    echo "❌ Server is not responding"
    exit 1
fi
echo ""

# Create a topic
echo "📝 Creating topic 'demo-topic'..."
./bin/yuclase-cli -cmd "CREATE demo-topic" >/dev/null
echo "✅ Topic created"
echo ""

# Enqueue some messages
echo "📤 Enqueuing messages..."
for i in {1..5}; do
    MSG="Message $i: Hello from Yuclase! $(date)"
    OFFSET=$(./bin/yuclase-cli -cmd "ENQUEUE demo-topic $MSG" 2>/dev/null)
    echo "  ✅ Enqueued: '$MSG' (offset: $OFFSET)"
done
echo ""

# List topics
echo "📋 Listing topics..."
TOPICS=$(./bin/yuclase-cli -cmd "TOPICS" 2>/dev/null)
echo "$TOPICS"
echo ""

# Get topic stats
echo "📊 Getting topic statistics..."
STATS=$(./bin/yuclase-cli -cmd "STATS demo-topic" 2>/dev/null)
echo "$STATS"
echo ""

# Listen for messages with consumer1
echo "👂 Listening for messages (consumer1)..."
MESSAGES=$(./bin/yuclase-cli -cmd "LISTEN demo-topic consumer1" 2>/dev/null)
echo "$MESSAGES"
echo ""

# Check consumer offset
echo "📍 Checking consumer offset..."
OFFSET=$(./bin/yuclase-cli -cmd "OFFSET demo-topic consumer1" 2>/dev/null)
echo "Consumer1 offset: $OFFSET"
echo ""

# Listen again (should get no new messages)
echo "👂 Listening again (should be empty)..."
MESSAGES2=$(./bin/yuclase-cli -cmd "LISTEN demo-topic consumer1" 2>/dev/null)
echo "$MESSAGES2"
echo ""

# Reset consumer offset and listen again
echo "🔄 Resetting consumer offset to 0..."
./bin/yuclase-cli -cmd "OFFSET demo-topic consumer1 0" >/dev/null
echo "✅ Offset reset"
echo ""

echo "👂 Listening after reset..."
MESSAGES3=$(./bin/yuclase-cli -cmd "LISTEN demo-topic consumer1" 2>/dev/null)
echo "$MESSAGES3"
echo ""

# List consumers
echo "👥 Listing consumers for demo-topic..."
CONSUMERS=$(./bin/yuclase-cli -cmd "CONSUMERS demo-topic" 2>/dev/null)
echo "$CONSUMERS"
echo ""

# Get overall queue stats
echo "📈 Getting overall queue statistics..."
QUEUE_STATS=$(./bin/yuclase-cli -cmd "STATS" 2>/dev/null)
echo "$QUEUE_STATS"
echo ""

echo "🎉 Demo completed successfully!"
echo ""
echo "💡 Try the interactive mode:"
echo "   ./bin/yuclase-cli"
echo ""
echo "📚 Available commands:"
echo "   ENQUEUE <topic> <message>     - Add message to topic"
echo "   LISTEN <topic> <consumer_id>  - Get messages for consumer"
echo "   OFFSET <topic> <consumer_id>  - Get consumer offset"
echo "   TOPICS                        - List all topics"
echo "   STATS [topic]                 - Get statistics"
echo "   CONSUMERS <topic>             - List consumers"
echo "   CREATE <topic>                - Create topic"
echo "   DELETE <topic>                - Delete topic"
echo "   PING                          - Test connectivity"
echo "   HELP                          - Show help"
echo "   QUIT                          - Exit"
