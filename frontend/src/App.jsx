import { useState } from "react";
import "./App.css";

function App() {
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(false);

  const checkHealth = async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/health");
      const data = await res.json();
      setHealth(data);
    } catch (error) {
      setHealth({ error: "API request failed" });
    } finally {
      setLoading(false);
    }
  };

  return (
    <main style={{ padding: "40px", fontFamily: "Arial" }}>
      <h1>Banking Microservices App</h1>
      <p>AKS + NGINX Ingress + Kafka + AI Analyzer + Monitoring</p>

      <button onClick={checkHealth}>
        {loading ? "Checking..." : "Check API Health"}
      </button>

      {health && (
        <pre style={{ marginTop: "20px" }}>
          {JSON.stringify(health, null, 2)}
        </pre>
      )}
    </main>
  );
}

export default App;