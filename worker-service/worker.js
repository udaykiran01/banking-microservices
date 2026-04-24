const express = require("express");
const axios = require("axios");
const { Kafka } = require("kafkajs");
const client = require("prom-client");

const API_BASE_URL = process.env.API_BASE_URL || "http://api-service:3000";
const KAFKA_BROKER =
  process.env.KAFKA_BROKER || "my-cluster-kafka-bootstrap.kafka.svc:9092";
const METRICS_PORT = process.env.METRICS_PORT || 4000;

const app = express();

// ADDED: Prometheus setup for worker-service
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// ADDED: Tracks successfully processed Kafka messages
const kafkaMessagesProcessed = new client.Counter({
  name: "kafka_messages_processed_total",
  help: "Total Kafka messages processed",
  labelNames: ["topic", "status"],
});

// ADDED: Tracks Kafka processing failures
const kafkaProcessingErrors = new client.Counter({
  name: "kafka_processing_errors_total",
  help: "Total Kafka processing errors",
  labelNames: ["topic", "error_type"],
});

register.registerMetric(kafkaMessagesProcessed);
register.registerMetric(kafkaProcessingErrors);

// ADDED: Worker metrics endpoint for Prometheus
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});

app.get("/health", (req, res) => {
  res.json({ status: "UP", service: "worker-service" });
});

app.listen(METRICS_PORT, () => {
  console.log(`Worker metrics running on port ${METRICS_PORT}`);
});

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

        console.log(`Processing transaction ${job.id} for ${job.customer_name}...`);

        // UPDATED: fixed wrong endpoint
        // OLD: /jobs/:id/process
        // NEW: /transactions/:id/process
        const response = await axios.put(
          `${API_BASE_URL}/transactions/${job.id}/process`
        );

        // ADDED: success metric
        kafkaMessagesProcessed.inc({
          topic,
          status: "success",
        });

        console.log(`Processed transaction ${job.id}: ${response.data.message}`);
      } catch (error) {
        // ADDED: error metric
        kafkaProcessingErrors.inc({
          topic,
          error_type: "api_update_failed",
        });

        console.error("Failed to process Kafka message:", error.message);

        if (error.response) {
          console.error("Status:", error.response.status);
          console.error("Response:", error.response.data);
        }
      }
    },
  });
}

startConsumer().catch((error) => {
  kafkaProcessingErrors.inc({
    topic: "transactions",
    error_type: "consumer_startup_failed",
  });

  console.error("Kafka consumer failed:", error.message);
});