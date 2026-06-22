import { useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

const API_URL = "http://banking-api-openshif-tbanking-ap-dev.apps.rm2.thpm.p1.openshiftapps.com";

export default function App() {
  const [transactions, setTransactions] = useState([]);
  const [lastUpdated, setLastUpdated] = useState("");

 async function loadTransactions() {
  try {
    const res = await axios.get(`${API_URL}/transactions`);
    console.log("API response:", res.data);

    setTransactions(res.data.transactions || []);
    setLastUpdated(new Date().toLocaleTimeString());
  } catch (err) {
    console.error("Failed to load transactions", err);
  }
}

  useEffect(() => {
    loadTransactions();
    const interval = setInterval(loadTransactions, 5000);
    return () => clearInterval(interval);
  }, []);

const total = transactions.length;
const fraud = transactions.filter((t) => Number(t.amount) >= 8000).length;
const pending = transactions.filter((t) => t.status === "PENDING").length;

  return (
    <div className="dashboard">
      <h1>🏦 Banking Fraud Detection Dashboard</h1>
      <p>Real-time fraud monitoring for banking transactions</p>
      <p>Last Updated: {lastUpdated}</p>

      <div className="cards">
        <div className="card">
          <h3>Total Transactions</h3>
          <h2>{total}</h2>
        </div>

        <div className="card fraud">
          <h3>Fraud Alerts</h3>
          <h2>{fraud}</h2>
        </div>

        <div className="card pending">
          <h3>Pending Reviews</h3>
          <h2>{pending}</h2>
        </div>
      </div>

      <h2>Recent Transactions</h2>

      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Customer</th>
            <th>Amount</th>
            <th>Type</th>
            <th>Status</th>
            <th>Risk</th>
          </tr>
        </thead>

        <tbody>
          {transactions.map((tx) => (
            <tr key={tx.id}>
              <td>{tx.id}</td>
              <td>{tx.customer_name}</td>
              <td>${tx.amount}</td>
              <td>{tx.type}</td>
              <td>{tx.status}</td>
              <td>
                <RiskBadge tx={tx} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RiskBadge({ tx }) {
  let risk = "LOW";

  if (tx.status === "FRAUD" || Number(tx.amount) >= 8000) {
    risk = "HIGH";
  } else if (Number(tx.amount) >= 2500) {
    risk = "MEDIUM";
  }

  return <span className={`badge ${risk.toLowerCase()}`}>{risk}</span>;
}
