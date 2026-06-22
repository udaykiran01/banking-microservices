from flask import Flask, request
app = Flask(__name__)
@app.route("/alertmanager", methods=["POST"])
def alertmanager_webhook():

    data = request.get_json()
    for alert in data["alerts"]:
        if data["status"] == "firing" :
            print("seveerity_high")
        labels = alert["labels"]

        #print(labels["alertname"])
        alertname = labels.get("alertname", "unknow_alert")
        namespace = labels.get("namespace", "unknown_namespace")
        print(namespace)
        print(alertname)

    return "received", 200
app.run(host="0.0.0.0", port=5001)  
#app.run()  


   # -d '{"status":"firing","alerts":[{"labels":{"alertname":"ApiHighErrorRate","namespace":"banking-app","service":"api-service","severity":"critical"}}]}'