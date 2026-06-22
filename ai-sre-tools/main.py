from fastapi import FastAPI
from kubernetes import client, config

NAMESPACE = "openshif-tbanking-ap-dev"

app = FastAPI(
    title="OpenShift SRE Tools",
    version="1.0.0"
)

config.load_incluster_config()
v1 = client.CoreV1Api()

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/pods")
def get_pods():
    pods = v1.list_namespaced_pod(namespace=NAMESPACE)

    return [
        {
            "name": pod.metadata.name,
            "status": pod.status.phase,
            "restarts": sum(
                cs.restart_count for cs in (pod.status.container_statuses or [])
            )
        }
        for pod in pods.items
    ]

@app.get("/events")
def get_events():
    events = v1.list_namespaced_event(namespace=NAMESPACE)

    return [
        {
            "reason": event.reason,
            "message": event.message,
            "type": event.type
        }
        for event in events.items[-20:]
    ]

apps_v1 = client.AppsV1Api()

@app.get("/deployments")
def get_deployments():
    deployments = apps_v1.list_namespaced_deployment(namespace=NAMESPACE)

    return [
        {
            "name": d.metadata.name,
            "desired_replicas": d.spec.replicas,
            "available_replicas": d.status.available_replicas or 0,
            "ready_replicas": d.status.ready_replicas or 0,
            "updated_replicas": d.status.updated_replicas or 0
        }
        for d in deployments.items
    ]