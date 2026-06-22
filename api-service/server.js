require("dd-trace").init({
  service: "banking-api",
  env: process.env.DD_ENV || "dev",
  version: process.env.DD_VERSION || "1.0.0",
  logInjection: true,
  runtimeMetrics: true
});

const express = require("express");
const { Pool } = require("pg");
const { Kafka } = require("kafkajs");
const client = require("prom-client");
const cors = require("cors");
const app = express();
const logger = require("./logger");
app.use(cors());
const PORT = process.env.PORT || 3000;

app.use(express.json());

logger.info({
  event: "api_started",
  service: "banking-api",
  timestamp: new Date().toISOString()
});

// Prometheus metrics
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// Existing metric: total HTTP requests
const httpRequestCounter = new client.Counter({
  name: "http_requests_total",
  help: "Total number of HTTP requests",
  labelNames: ["method", "route", "status"],
});

register.registerMetric(httpRequestCounter);

// ADDED: Measures API response time / latency
const httpRequestDuration = new client.Histogram({
  name: "http_request_duration_seconds",
  help: "HTTP request duration in seconds",
  labelNames: ["method", "route", "status"],
  buckets: [0.1, 0.3, 0.5, 1, 2, 5],
});

register.registerMetric(httpRequestDuration);

// ADDED: Business metric for created banking transactions
const transactionCounter = new client.Counter({
  name: "banking_transactions_total",
  help: "Total number of banking transactions created",
  labelNames: ["type", "status"],
});

register.registerMetric(transactionCounter);

// ADDED: Tracks application errors by route/component
const appErrorCounter = new client.Counter({
  name: "banking_app_errors_total",
  help: "Application errors by endpoint and component",
  labelNames: ["route", "component"],
});





register.registerMetric(appErrorCounter);

// UPDATED: Tracks both request count and request duration
app.use((req, res, next) => {
  const end = httpRequestDuration.startTimer();

  res.on("finish", () => {
    const labels = {
      method: req.method,
      route: req.path,
      status: String(res.statusCode),
    };

    httpRequestCounter.inc(labels);
    end(labels);
  });

  next();
});

const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || "5432", 10),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl:
    process.env.DB_SSL === "true"
      ? { rejectUnauthorized: false }
      : false,
});

const kafka = new Kafka({
  clientId: "api-service",
  brokers: [
    process.env.KAFKA_BROKER || "my-cluster-kafka-bootstrap.kafka.svc:9092",
  ],
});

const producer = kafka.producer();
let kafkaProducerReady = false;

async function startProducer() {
  await producer.connect();
  kafkaProducerReady = true;
  console.log("Kafka producer connected");
}

startProducer().catch((err) => {
  // ADDED: Track Kafka startup failure
  appErrorCounter.inc({
    route: "startup",
    component: "kafka-producer",
  });

  console.error("Failed to connect Kafka producer:", err.message);
});

function alive(req, res) {
  res.status(200).json({
    status: "UP",
    service: "api-service",
  });
}

async function checkKafka() {
  if (!kafkaProducerReady) {
    throw new Error("Kafka producer is not connected");
  }

  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.listTopics();
  } finally {
    await admin.disconnect();
  }
}

async function ready(req, res) {
  const checks = {
    database: "UNKNOWN",
    kafka: "UNKNOWN",
  };

  try {
    await pool.query("SELECT 1");
    checks.database = "CONNECTED";
  } catch (error) {
    checks.database = "DISCONNECTED";
    appErrorCounter.inc({
      route: req.path,
      component: "database",
    });

    return res.status(503).json({
      status: "DOWN",
      service: "api-service",
      checks,
      error: error.message,
    });
  }

  try {
    await checkKafka();
    checks.kafka = "CONNECTED";
  } catch (error) {
    checks.kafka = "DISCONNECTED";
    appErrorCounter.inc({
      route: req.path,
      component: "kafka",
    });

    return res.status(503).json({
      status: "DOWN",
      service: "api-service",
      checks,
      error: error.message,
    });
  }

  return res.status(200).json({
    status: "UP",
    service: "api-service",
    checks,
  });
}

app.get("/health", alive);
app.get("/api/health", alive);
app.get("/ready", ready);
app.get("/api/ready", ready);

app.get("/metrics", async (req, res) => {
  try {
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (error) {
    // ADDED: Track metrics endpoint failure
    appErrorCounter.inc({
      route: "/metrics",
      component: "prometheus",
    });

    res.status(500).end(error.message);
  }
});

app.post("/transactions", async (req, res) => {
  try {
    const { customerName, amount, type } = req.body;

    if (!customerName || !amount || !type) {
      // ADDED: Track bad transaction requests
      appErrorCounter.inc({
        route: "/transactions",
        component: "validation",
      });

      return res.status(400).json({
        error: "customerName, amount, and type are required",
      });
    }

    const result = await pool.query(
      `
      INSERT INTO transactions (customer_name, amount, type, status)
      VALUES ($1, $2, $3, 'PENDING')
      RETURNING id, customer_name, amount, type, status, created_at, processed_at
      `,
      [customerName, amount, type]
    );

    const transaction = result.rows[0];

    await producer.send({
      topic: "transactions",
      messages: [
        {
          key: String(transaction.id),
          value: JSON.stringify({
            id: transaction.id,
            customer_name: transaction.customer_name,
            amount: transaction.amount,
            type: transaction.type,
            status: transaction.status,
          }),
        },
      ],
    });

    // ADDED: Track successful banking transaction creation
    transactionCounter.inc({
      type: transaction.type,
      status: transaction.status,
    });

    logger.info({
  event: "transaction_created",
  service: "banking-api",
  transaction_id: transaction.id,
  customer_name: transaction.customer_name,
  amount: transaction.amount,
  transaction_type: transaction.type,
  status: transaction.status,
  timestamp: new Date().toISOString()
});

    return res.status(201).json({
      message: "Transaction created successfully",
      transaction,
    });
  } catch (error) {
    // ADDED: Track transaction creation failure

    logger.error({
  event: "transaction_create_failed",
  service: "banking-api",
  error: error.message,
  timestamp: new Date().toISOString()
});
    appErrorCounter.inc({
      route: "/transactions",
      component: "api-db-kafka",
    });

    return res.status(500).json({
      error: "Failed to create transaction",
      details: error.message,
    });
  }
});

app.put("/transactions/:id/process", async (req, res) => {
  try {
    const { id } = req.params;
    const { fraud, fraud_probability } = req.body;

    const fraudScore = fraud_probability;
    const fraudStatus = fraud ? "FRAUD" : "CLEAR";
    const status = fraud ? "FLAGGED" : "PROCESSED";

    const result = await pool.query(
      `
      UPDATE transactions
      SET status = $1,
          processed_at = NOW(),
          fraud_score = $2,
          fraud_status = $3
      WHERE id = $4
      RETURNING *
      `,
      [status, fraudScore, fraudStatus, id]
    );

    res.json({
      message: "Transaction processed successfully",
      transaction: result.rows[0],
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/transactions", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, customer_name, amount, type, status, created_at, processed_at
      FROM transactions
      ORDER BY id ASC
    `);

    res.json({
      count: result.rows.length,
      transactions: result.rows,
    });
  } catch (error) {
    // ADDED: Track transaction fetch failure
    appErrorCounter.inc({
      route: "/transactions",
      component: "database",
    });

    res.status(500).json({
      error: "Failed to fetch transactions",
      details: error.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`API service running on port ${PORT}`);
});
