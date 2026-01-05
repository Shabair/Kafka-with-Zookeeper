import express from "express";
import cors from "cors";
import { Kafka, logLevel } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

// Get broker configuration from environment or use defaults
const KAFKA_BROKERS = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',') 
  : ['localhost:9091', 'localhost:9092', 'localhost:9093'];

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'payment-service';

const app = express();

// CORS Configuration
const corsOptions = {
  origin: "http://localhost:3001",
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true, // If you need cookies/auth headers
};

app.use(cors(corsOptions));
app.use(express.json());

// Add request logging middleware
app.use((req, res, next) => {
  console.log(`${req.method} ${req.path}`);
  next();
});

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

// Create producer
const producer = kafka.producer();
let isProducerConnected = false;

// Kafka connection with retry logic
const connectToKafka = async (retries = 5) => {
  for (let i = 0; i < retries; i++) {
    try {
      await producer.connect();
      isProducerConnected = true;
      console.log("✅ Kafka producer connected successfully.");
      return true;
    } catch (err) {
      console.error(`❌ Kafka connection attempt ${i + 1}/${retries} failed:`, err.message);
      
      if (i < retries - 1) {
        // Wait before retrying (exponential backoff)
        const delay = Math.min(1000 * Math.pow(2, i), 10000);
        console.log(`Retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  console.error("❌ Failed to connect to Kafka after all retries");
  isProducerConnected = false;
  return false;
};

// Health check endpoint
app.get("/payment-service/health", (req, res) => {
  const status = {
    service: "payment-service",
    status: "running",
    port: process.env.PORT || 8000,
    kafka: isProducerConnected ? "connected" : "disconnected",
    timestamp: new Date().toISOString()
  };
  return res.status(200).json(status);
});

app.get("/payment-service", (req, res) => {
  console.log("GET request received at /payment-service");
  return res.status(200).json({ 
    message: "Payment service is running",
    endpoints: {
      health: "GET /payment-service/health",
      processPayment: "POST /payment-service"
    }
  });
});

// Process payment endpoint
app.post("/payment-service", async (req, res, next) => {
  try {
    // Validate request body
    if (!req.body || !req.body.cart) {
      return res.status(400).json({ 
        error: "Bad request", 
        message: "Cart data is required" 
      });
    }

    const { cart } = req.body;
    
    // Validate cart structure
    if (!Array.isArray(cart)) {
      return res.status(400).json({ 
        error: "Bad request", 
        message: "Cart must be an array" 
      });
    }

    // Check if Kafka producer is connected
    if (!isProducerConnected) {
      console.warn("⚠️ Kafka producer not connected, attempting to reconnect...");
      const reconnected = await connectToKafka(1);
      
      if (!reconnected) {
        return res.status(503).json({ 
          error: "Service unavailable", 
          message: "Payment system is temporarily unavailable. Please try again later." 
        });
      }
    }

    // User ID - in a real app, this would come from authentication
    const userId = req.user?.id || 999999; // Replace with actual auth

    // TODO: Implement actual payment processing logic here
    console.log(`Processing payment for user ${userId}, cart items: ${cart.length}`);

    // Calculate total
    const total = cart.reduce((sum, item) => sum + (item.price || 0), 0);
    
    // Send payment success message to Kafka
    try {
      await producer.send({
        topic: "payment-successful",
        messages: [{
          key: userId.toString(), // Partition by user ID
          value: JSON.stringify({ 
            userId, 
            cart,
            total,
            timestamp: new Date().toISOString(),
            paymentId: `pay_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
          })
        }],
      });
      
      console.log(`✅ Payment event sent to Kafka for user ${userId}`);
      
      return res.status(200).json({ 
        success: true,
        message: "Payment processed successfully!",
        userId,
        total,
        items: cart.length,
        paymentId: `pay_${Date.now()}`
      });
      
    } catch (kafkaError) {
      console.error("❌ Failed to send message to Kafka:", kafkaError);
      
      // In a real application, you might want to store failed payments in a database
      // for later retry (dead letter queue pattern)
      
      return res.status(202).json({ 
        success: true, 
        message: "Payment processed but notification failed. Our team will follow up.",
        userId,
        total,
        note: "Payment recorded but system notification pending"
      });
    }
    
  } catch (error) {
    console.error("❌ Payment processing error:", error);
    next(error); // Pass to error handling middleware
  }
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    error: "Not found", 
    message: `Route ${req.method} ${req.path} not found` 
  });
});

// Improved error handling middleware
app.use((error, req, res, next) => {
  console.error("🔥 Unhandled error:", error);
  
  const statusCode = error.status || error.statusCode || 500;
  const message = error.message || "Internal server error";
  
  res.status(statusCode).json({
    error: "Internal server error",
    message: message,
    ...(process.env.NODE_ENV === 'development' && { stack: error.stack })
  });
});

// Graceful shutdown handling
const gracefulShutdown = async (signal) => {
  console.log(`\n${signal} received. Shutting down gracefully...`);
  
  try {
    // Disconnect Kafka producer
    if (isProducerConnected) {
      await producer.disconnect();
      console.log("✅ Kafka producer disconnected.");
    }
    
    console.log("👋 Payment service shutdown complete.");
    process.exit(0);
  } catch (error) {
    console.error("❌ Error during shutdown:", error);
    process.exit(1);
  }
};

// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught errors
process.on('unhandledRejection', (reason, promise) => {
  console.error('🚨 Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('🚨 Uncaught Exception:', error);
  gracefulShutdown('UNCAUGHT_EXCEPTION');
});

// Start server
const PORT = process.env.PORT || 8000;

const startServer = async () => {
  try {
    // Connect to Kafka first
    await connectToKafka();
    
    // Start Express server
    app.listen(PORT, () => {
      console.log(`
🚀 Payment Service Started!
📡 Port: ${PORT}
🔗 Health Check: http://localhost:${PORT}/payment-service/health
📊 Kafka Status: ${isProducerConnected ? 'Connected' : 'Disconnected'}
      `);
    });
    
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
};

startServer();

// Export for testing if needed
export { app, producer, connectToKafka };