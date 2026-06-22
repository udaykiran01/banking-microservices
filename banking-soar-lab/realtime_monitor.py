from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY")

def send_slack(alert):
    text = f"""
🚨 *Banking App Alert*
Alert: {alert["labels"].get("alertname")}
Severity: {alert["labels"].get("severity")}
Service: {alert["labels"].get("service")}
Summary: {alert["annotations"].get("summary")}
"""
    requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)

def send_pagerduty(alert):
    payload = {
        "routing_key": PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": alert["fingerprint"],
        "payload": {
            "summary": alert["annotations"].get("summary", "Banking app alert"),
            "source": alert["labels"].get("service", "banking-app"),
            "severity": "critical",
            "component": alert["labels"].get("service", "api-service"),
            "group": alert["labels"].get("namespace", "banking-app"),
            "class": alert["labels"].get("alertname", "PrometheusAlert")
        }
    }

    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json=payload,
        timeout=10
    )

@app.route("/alert", methods=["POST"])
def alert():
    data = request.json

    for alert in data.get("alerts", []):
        severity = alert["labels"].get("severity")

        send_slack(alert)

        if severity == "critical":
            send_pagerduty(alert)

    return jsonify({"status": "processed"}), 200

app.run(host="0.0.0.0", port=8080)