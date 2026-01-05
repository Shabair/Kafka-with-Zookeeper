import { Kafka, logLevel } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

// Get broker configuration from environment or use defaults
const KAFKA_BROKERS = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : ["localhost:9091", "localhost:9092", "localhost:9093"];

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "order-service";
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "order-successful";

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
    console.log("🚀 Starting order service...");

    // Connect to Kafka - WITH AWAIT!
    await producer.connect();
    console.log("✅ Producer connected");

    await consumer.connect();
    console.log("✅ Consumer connected");

    // Subscribe to topic - WITH AWAIT!
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });
    console.log("✅ Subscribed to topic: payment-successful");

    // Run the consumer
    await consumer.run({
      partitionsConsumedConcurrently: 2,

      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        try {
          // Parse the message
          const value = message.value.toString();
          const data = JSON.parse(value);

          // Extract data with safe defaults
          const { userId, cart = [], total: cartTotal } = data;

          // Validate required data
          if (!userId) {
            console.warn("⚠️ Missing userId in payment message, skipping...");
            return;
          }

          // Calculate total
          const total =
            cartTotal ||
            cart.reduce((acc, item) => acc + (item?.price || 0), 0);

          // Generate order ID (in real app, this would come from a database)
          const orderId = `ORD_${Date.now()}_${Math.random()
            .toString(36)
            .substr(2, 9)
            .toUpperCase()}`;

          // Create order object
          const order = {
            userId,
            orderId,
            total,
            items: cart.length,
            status: "confirmed",
            createdAt: new Date().toISOString(),
            paymentData: {
              timestamp: data.timestamp,
              paymentId: data.paymentId,
            },
          };

          // Send order confirmation to Kafka - WITH AWAIT!
          await producer.send({
            topic: KAFKA_TOPIC,
            //compression: CompressionTypes.GZIP, // ← ADD THIS LINE
            messages: [
              {
                key: userId.toString(), // Partition by user ID
                value: JSON.stringify(order),
                headers: {
                  service: "order-service",
                  "event-type": "order-confirmed",
                },
              },
            ],
          });

          console.log(`
            📦 Order Created:
              User ID: ${userId}
              Order ID: ${orderId}
              Total Amount: $${total.toFixed(2)}
              Items: ${cart.length}
              Status: Confirmed
              From Topic: ${topic}:${partition}@${message.offset}
          `);

          // Send heartbeat to keep consumer alive
          await heartbeat();

          // TODO: In a real application, you would:
          // 1. Save order to database
          // 2. Update inventory
          // 3. Send confirmation email
          // 4. Trigger shipping process
        } catch (error) {
          console.error(`❌ Error processing payment message:`, error);
          console.error(
            `Message details: topic=${topic}, partition=${partition}, offset=${message.offset}`
          );

          // Implement dead-letter queue logic here
          // await sendToDeadLetterQueue({
          //   topic,
          //   partition,
          //   message,
          //   error: error.message,
          //   service: 'order-service'
          // });
        }
      },
    });

    isRunning = true;
    console.log("✅ Order service is running and processing payments...");
    console.log(`📤 Producing to topic: ${KAFKA_TOPIC}`);
  } catch (err) {
    console.error("❌ Error in order service:", err?.message || err);
    await gracefulShutdown();
    process.exit(1);
  }
};

// Graceful shutdown function
const gracefulShutdown = async () => {
  if (!isRunning) return;

  console.log("\n🛑 Shutting down order service...");
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
    console.log("👋 Order service shutdown complete");
  }
};

// Handle process termination signals
process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

// Handle unhandled promise rejections
process.on("unhandledRejection", (reason, promise) => {
  console.error("🚨 Unhandled Rejection at:", promise, "reason:", reason);
});

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  console.error("🚨 Uncaught Exception:", error);
  gracefulShutdown();
  process.exit(1);
});

// Start the service
run().catch(async (error) => {
  console.error("🔥 Fatal error:", error);
  await gracefulShutdown();
  process.exit(1);
});

// Optional: Status function
const getServiceStatus = () => ({
  service: "order-service",
  isRunning,
  consumerGroup: KAFKA_CLIENT_ID,
  consumingFrom: "payment-successful",
  producingTo: KAFKA_TOPIC,
  brokers: KAFKA_BROKERS,
  timestamp: new Date().toISOString(),
});

// Export for testing
export { run, gracefulShutdown, getServiceStatus, producer, consumer };
