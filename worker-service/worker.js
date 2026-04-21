const axios = require("axios");
const { Kafka } = require("kafkajs");

const API_BASE_URL = process.env.API_BASE_URL || "http://api-service:3000";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "my-cluster-kafka-bootstrap.kafka.svc:9092";

const kafka = new Kafka({
  clientId: "worker-service",
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "transaction-workers" });

async function startConsumer() {
  await consumer.connect();
  console.log("Kafka consumer connected");

  await consumer.subscribe({ topic: "transactions", fromBeginning: true });
  console.log("Subscribed to Kafka topic: transactions");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const job = JSON.parse(message.value.toString());
        console.log(`Processing job ${job.id} for ${job.customer_name}...`);

        const response = await axios.put(`${API_BASE_URL}/jobs/${job.id}/process`);

        console.log(`Processed job ${job.id}: ${response.data.message}`);
      } catch (error) {
        console.error("Failed to process Kafka message:", error.message);
      }
    },
  });
}

startConsumer().catch((error) => {
  console.error("Kafka consumer failed:", error.message);
});