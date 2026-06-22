const express = require("express");
const axios = require("axios");
const { Kafka } = require("kafkajs");
const client = require("prom-client");
const fs = require("fs");

const logPath = "/app/logs/worker.log";

function appLog(level, message, data = {}) {
  const log = {
    timestamp: new Date().toISOString(),
    level,
    service: "worker-service",
    message,
    ...data,
  };

  console.log(JSON.stringify(log));

  try {
    fs.appendFileSync(logPath, JSON.stringify(log) + "\n");
  } catch (err) {
    console.error("Failed to write app log", err.message);
  }
}

const API_BASE_URL = process.env.API_BASE_URL || "http://banking-api";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:9092";
const METRICS_PORT = process.env.METRICS_PORT || 4000;
const FRAUD_SERVICE_URL =
  process.env.FRAUD_SERVICE_URL || "http://fraud-service:8000/predict";

const app = express();

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const kafkaMessagesProcessed = new client.Counter({
  name: "kafka_messages_processed_total",
  help: "Total Kafka messages processed",
  labelNames: ["topic", "status"],
});

const kafkaProcessingErrors = new client.Counter({
  name: "kafka_consumer_errors_total",
  help: "Total Kafka consumer errors",
  labelNames: ["topic", "error_type"],
});

register.registerMetric(kafkaMessagesProcessed);
register.registerMetric(kafkaProcessingErrors);

app.get("/health", (req, res) => {
  res.json({
    status: "UP",
    service: "worker-service",
  });
});

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});

app.listen(METRICS_PORT, () => {
  appLog("INFO", "Worker metrics server started", {
    port: METRICS_PORT,
  });
});

const kafka = new Kafka({
  clientId: "worker-service",
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "transaction-workers" });

function buildFraudPayload(job) {
  return {
    amount: Number(job.amount || 0),
    account_age_days: Number(job.account_age_days || 30),
    transaction_count_24h: Number(job.transaction_count_24h || 1),
    avg_transaction_amount: Number(
      job.avg_transaction_amount || job.amount || 0
    ),
  };
}

async function startConsumer() {
  await consumer.connect();

  appLog("INFO", "Kafka consumer connected", {
    broker: KAFKA_BROKER,
  });

  await consumer.subscribe({
    topic: "transactions",
    fromBeginning: false,
  });

  appLog("INFO", "Subscribed to Kafka topic", {
    topic: "transactions",
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let job;

      try {
        job = JSON.parse(message.value.toString());

        appLog("INFO", "Message consumed", {
          topic,
          partition,
          transactionId: job.id,
          customerName: job.customer_name,
          amount: job.amount,
        });

        const fraudPayload = buildFraudPayload(job);

        appLog("INFO", "Calling fraud service", {
          transactionId: job.id,
          fraudServiceUrl: FRAUD_SERVICE_URL,
        });

        const fraudResponse = await axios.post(
          FRAUD_SERVICE_URL,
          fraudPayload,
          { timeout: 5000 }
        );

        appLog("INFO", "Fraud service response received", {
          transactionId: job.id,
          fraud: fraudResponse.data.fraud,
          fraudProbability: fraudResponse.data.fraud_probability,
        });

        const response = await axios.put(
          `${API_BASE_URL}/transactions/${job.id}/process`,
          {
            fraud: fraudResponse.data.fraud,
            fraud_probability: fraudResponse.data.fraud_probability,
          }
        );

        kafkaMessagesProcessed.inc({
          topic,
          status: fraudResponse.data.fraud ? "fraud_detected" : "success",
        });

        appLog("INFO", "Transaction processed successfully", {
          transactionId: job.id,
          message: response.data.message,
        });
      } catch (error) {
        kafkaProcessingErrors.inc({
          topic,
          error_type: "fraud_or_api_update_failed",
        });

        appLog("ERROR", "Failed to process Kafka message", {
          transactionId: job?.id || null,
          error: error.message,
          status: error.response?.status || null,
          response: error.response?.data || null,
        });
      }
    },
  });
}

startConsumer().catch((error) => {
  kafkaProcessingErrors.inc({
    topic: "transactions",
    error_type: "consumer_startup_failed",
  });

  appLog("ERROR", "Kafka consumer startup failed", {
    error: error.message,
  });
});