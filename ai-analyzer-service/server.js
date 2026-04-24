const express = require("express");

const app = express();
const PORT = process.env.PORT || 5000;

app.use(express.json({ limit: "2mb" }));

app.get("/health", (req, res) => {
  res.json({ status: "UP", service: "ai-analyzer-service" });
});

app.post("/alert", async (req, res) => {
  try {
    const alerts = req.body.alerts || [];

    const prompt = `
You are a Senior DevOps/SRE Engineer.

Analyze these Prometheus alerts from an AKS banking microservices app:

${JSON.stringify(alerts, null, 2)}

Return:
1. Incident summary
2. Root cause
3. Business impact
4. Immediate fix
5. Kubernetes commands
6. Long-term prevention
`;

    const response = await fetch(
      `${process.env.AZURE_OPENAI_ENDPOINT}/openai/deployments/${process.env.AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version=2024-02-15-preview`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "api-key": process.env.AZURE_OPENAI_KEY,
        },
        body: JSON.stringify({
          messages: [
            {
              role: "system",
              content:
                "You analyze Kubernetes, Kafka, Prometheus, and DevOps incidents.",
            },
            {
              role: "user",
              content: prompt,
            },
          ],
          temperature: 0.2,
        }),
      }
    );

    const data = await response.json();
    const analysis = data.choices?.[0]?.message?.content;

    console.log("===== AI INCIDENT ANALYSIS =====");
    console.log(analysis);
    console.log("================================");

    res.json({
      status: "received",
      analysis,
    });
  } catch (error) {
    console.error("AI analyzer failed:", error.message);
    res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`AI Analyzer running on port ${PORT}`);
});