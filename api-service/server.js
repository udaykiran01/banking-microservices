const express = require("express");
const { Pool } = require("pg");
const { Kafka } = require("kafkajs");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || "5432", 10),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: {
    rejectUnauthorized: false,
  },
});

const kafka = new Kafka({
  clientId: "api-service",
  brokers: [process.env.KAFKA_BROKER || "my-cluster-kafka-bootstrap.kafka.svc:9092"],
});

const producer = kafka.producer();

async function startProducer() {
  await producer.connect();
  console.log("Kafka producer connected");
}

startProducer().catch((err) => {
  console.error("Failed to connect Kafka producer:", err.message);
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.status(200).json({
      status: "UP",
      service: "api-service",
      database: "CONNECTED",
    });
  } catch (error) {
    res.status(500).json({
      status: "DOWN",
      service: "api-service",
      database: "DISCONNECTED",
      error: error.message,
    });
  }
});

app.post("/transactions", async (req, res) => {
  try {
    const { customerName, amount, type } = req.body;

    if (!customerName || !amount || !type) {
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

    return res.status(201).json({
      message: "Transaction created successfully",
      transaction,
    });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to create transaction",
      details: error.message,
    });
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
    res.status(500).json({
      error: "Failed to fetch transactions",
      details: error.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`API service running on port ${PORT}`);
});