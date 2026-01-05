import { Kafka, logLevel } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

// Get broker configuration from environment or use defaults
const KAFKA_BROKERS = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',') 
  : ['localhost:9091', 'localhost:9092', 'localhost:9093'];

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'analytic-service';

// Create Kafka instance with multiple brokers
const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
  },
});

// Create consumer
const consumer = kafka.consumer({ 
  groupId: KAFKA_CLIENT_ID,
  // Optional: Add session timeout and heartbeat configuration
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  // Allow the consumer to read from the beginning if it's a new consumer group
  allowAutoTopicCreation: false,
});

let isRunning = false;

const run = async () => {
  try {
    console.log('📊 Starting analytic service consumer...');
    
    // Connect to Kafka
    await consumer.connect();
    console.log('✅ Connected to Kafka brokers:', KAFKA_BROKERS);
    
    // Subscribe to topic
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true
    });
    console.log('✅ Subscribed to topic: payment-successful');
    
    // Run the consumer
    await consumer.run({
      // Optional: Configure how many messages to process in parallel
      partitionsConsumedConcurrently: 3,
      
      eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        try {
          // Parse the message
          const value = message.value.toString();
          const data = JSON.parse(value);
          
          // Extract data with defaults to prevent errors
          const { userId, cart = [], total: providedTotal, timestamp } = data;
          
          // Calculate total from cart if not provided
          const total = providedTotal !== undefined 
            ? providedTotal 
            : cart.reduce((acc, item) => acc + (item?.price || 0), 0);
          
          // Get additional metadata
          const paymentId = data.paymentId || 'N/A';
          const itemCount = cart.length;
          
          // Log analytic data
          console.log(`
📈 Analytic Consumer:
  User ID: ${userId}
  Payment ID: ${paymentId}
  Total Paid: $${total.toFixed(2)}
  Items Purchased: ${itemCount}
  Timestamp: ${timestamp || new Date().toISOString()}
  Topic: ${topic}
  Partition: ${partition}
  Offset: ${message.offset}
          `);
          
          // Optional: Send heartbeat to keep the consumer alive during long processing
          await heartbeat();
          
          // Optional: Implement analytic processing logic here
          // For example, you could:
          // 1. Store analytics in a database
          // 2. Update real-time dashboards
          // 3. Send notifications to other services
          // 4. Calculate running totals, averages, etc.
          
          // Simulate some processing time
          // await processAnalytics(data);
          
        } catch (error) {
          console.error(`❌ Error processing message from topic ${topic}, partition ${partition}, offset ${message.offset}:`, error);
          
          // In a real application, you might want to:
          // 1. Log to a dead letter queue
          // 2. Send to a monitoring service
          // 3. Retry with exponential backoff
          
          // Example dead letter queue logic:
          // await sendToDeadLetterQueue(topic, partition, message, error);
        }
      },
      
      // Optional: Add batch processing if needed
      // eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
      //   for (let message of batch.messages) {
      //     if (!isRunning() || isStale()) break;
      //     // Process each message
      //     await heartbeat();
      //   }
      // }
    });
    
    isRunning = true;
    console.log('🚀 Analytic service consumer is running and ready to process payments...');
    
  } catch (err) {
    console.error('❌ Error starting analytic service:', err?.message || err);
    await gracefulShutdown();
    process.exit(1);
  }
};

// Optional: Analytics processing function
const processAnalytics = async (paymentData) => {
  // Simulate async processing
  return new Promise(resolve => setTimeout(resolve, 100));
  
  // In a real application:
  // 1. Update user purchase history in database
  // 2. Update product sales statistics
  // 3. Trigger recommendation engine updates
  // 4. Send data to data warehouse
};

// Graceful shutdown function
const gracefulShutdown = async () => {
  if (!isRunning) return;
  
  console.log('\n🛑 Shutting down analytic service...');
  isRunning = false;
  
  try {
    // Disconnect consumer
    await consumer.disconnect();
    console.log('✅ Consumer disconnected successfully');
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
  } finally {
    console.log('👋 Analytic service shutdown complete');
  }
};

// Handle process termination signals
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('🚨 Unhandled Rejection at:', promise, 'reason:', reason);
  gracefulShutdown();
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('🚨 Uncaught Exception:', error);
  gracefulShutdown();
  process.exit(1);
});

// Start the consumer
run().catch(async (error) => {
  console.error('🔥 Fatal error in run():', error);
  await gracefulShutdown();
  process.exit(1);
});

// Optional: Health check/status monitoring
const getConsumerStatus = () => ({
  isRunning,
  groupId: KAFKA_CLIENT_ID,
  topic: 'payment-successful',
  brokers: KAFKA_BROKERS,
  timestamp: new Date().toISOString()
});

// Export for testing if needed
export { run, gracefulShutdown, getConsumerStatus, consumer };