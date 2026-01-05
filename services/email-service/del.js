import { Kafka, logLevel } from "kafkajs";
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

// Create producer
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: KAFKA_CLIENT_ID });

const run = async () => {
  try {
    producer.connect();
    consumer.connect();
    consumer.subscribe({
      topic: "order-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, dumyOrderID } = JSON.parse(value);


        const dumyEmailID = 98798754444;

        producer.send({
          topic: KAFKA_TOPIC,
          messages: [{ value: JSON.stringify({ userId, dumyEmailID }) }],
        });

        console.log(
          `Email to customer: User ID: ${userId} and email ID: ${dumyEmailID}`
        );
      },
    });
  } catch (err) {
    console.log("Error: ", err?.message || err);
  }
};

run();
