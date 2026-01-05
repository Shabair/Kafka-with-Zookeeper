import { Kafka, logLevel } from "kafkajs";
import lz4 from "lz4";
import pkg from "kafkajs";

const { 
  CompressionTypes,
  CompressionCodecs
} = pkg;


CompressionCodecs[CompressionTypes.LZ4] = () => ({
  compress: (buffer) => lz4.encode(buffer),
  decompress: (buffer) => lz4.decode(buffer),
});

import dotenv from "dotenv";

dotenv.config();

// Get broker configuration from environment or use defaults
const KAFKA_BROKERS = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : ["localhost:9091", "localhost:9092", "localhost:9093"];

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "email-service";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "email-successful";

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

// Create producer and consumer
const producer = kafka.producer();
const consumer = kafka.consumer({ 
  groupId: KAFKA_CLIENT_ID,
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

let isRunning = false;

const run = async () => {
  try {
    console.log("🚀 Starting EMAIL service..."); // Changed from "order service"
    
    // Connect to Kafka - WITH AWAIT!
    await producer.connect();
    console.log("✅ Producer connected");
    
    await consumer.connect();
    console.log("✅ Consumer connected");
    
    // Subscribe to topic - WITH AWAIT!
    await consumer.subscribe({
      topic: "order-successful", // Make sure this is correct
      fromBeginning: true,
    });
    console.log("✅ Subscribed to topic: order-successful"); // Fixed log message
    
    // Run the consumer
    await consumer.run({
      partitionsConsumedConcurrently: 2,
      
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        try {
          // Parse the message
          const value = message.value.toString();
          console.log("Received message:", value);
          const data = JSON.parse(value);
          
          // Process the order data for email
          const { userId, orderId, total } = data;
          
          console.log(`📧 Would send email for order ${orderId} to user ${userId} for amount $${total}`);
          
          // Send heartbeat to keep consumer alive
          await heartbeat();
          
        } catch (error) {
          console.error(`❌ Error processing order message:`, error);
          console.error(`Message details: topic=${topic}, partition=${partition}, offset=${message.offset}`);
        }
      },
    });
    
    isRunning = true;
    console.log("✅ EMAIL service is running and processing orders..."); // Fixed log
    console.log(`📤 Producing to topic: ${KAFKA_TOPIC}`);
    
  } catch (err) {
    console.error("❌ Error in EMAIL service:", err?.message || err); // Fixed log
    await gracefulShutdown();
    process.exit(1);
  }
};

// Graceful shutdown function
const gracefulShutdown = async () => {
  if (!isRunning) return;
  
  console.log("\n🛑 Shutting down EMAIL service..."); // Fixed log
  isRunning = false;
  
  try {
    // Disconnect consumer
    await consumer.disconnect();
    console.log("✅ Consumer disconnected");
    
    // Disconnect producer
    await producer.disconnect();
    console.log("✅ Producer disconnected");
    
  } catch (error) {
    console.error("❌ Error during shutdown:", error);
  } finally {
    console.log("👋 EMAIL service shutdown complete"); // Fixed log
  }
};

// Handle process termination signals
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('🚨 Unhandled Rejection at:', promise, 'reason:', reason);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('🚨 Uncaught Exception:', error);
  gracefulShutdown();
  process.exit(1);
});

// Start the service
run().catch(async (error) => {
  console.error('🔥 Fatal error:', error);
  await gracefulShutdown();
  process.exit(1);
});

// Optional: Status function - FIXED service name
const getServiceStatus = () => ({
  service: 'email-service', // Changed from 'order-service'
  isRunning,
  consumerGroup: KAFKA_CLIENT_ID,
  consumingFrom: 'order-successful', // Changed from 'payment-successful'
  producingTo: KAFKA_TOPIC,
  brokers: KAFKA_BROKERS,
  timestamp: new Date().toISOString()
});

// Export for testing
export { run, gracefulShutdown, getServiceStatus, producer, consumer };