from flask import Flask, request
import requests

app = Flask(__name__)

SLACK_WEBHOOK = "YOUR_SLACK_WEBHOOK"

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.json

    severity = data.get("severity")
    user = data.get("user")
    ip = data.get("ip")
    event = data.get("event")

    print(f"[ALERT] {event} - {user} - {ip}")

    if severity == "high":

        message = {
            "text": f"""
🚨 AWS Federation Alert

User: {user}
IP: {ip}
Event: {event}
Severity: HIGH
"""
        }

        requests.post(SLACK_WEBHOOK, json=message)

    return "ok", 200

app.run(host="0.0.0.0", port=5001)