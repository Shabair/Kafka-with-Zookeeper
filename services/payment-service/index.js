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

app.use(cors({
    origin: "http://localhost:3001",
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"]
}));

app.use(express.json());

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
  
  // Create admin client
const producer = kafka.producer();

app.get("/payment-service", (req, res)=>{
    console.log("In get of payment");

    return res.status(200).send("success")
});

app.post("/payment-service", async  (req, res )=>{
    const {cart} = req.body; 

    //User login check etc
    const userId = 123;

    //TODO: payment
    console.log("Payment Method called");

    //TODO: Kafka

    return res.status(200).send("Payment Successful! ");
});


app.use((err, req, res, next)=>{
    res.status(err.status || 500).send(err.message );
} );


app.listen(8000, ()=>{
    console.log("App is running on port: 8000");
});