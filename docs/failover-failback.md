# Failover and Failback Runbook

## Primary and Secondary

Primary environment: Azure AKS.

Secondary environment: AWS EKS or OpenShift, depending on the active recovery target for the incident.

Users enter through Azure Front Door, so the public DNS name remains stable during failover and failback. Front Door should use the AKS ingress public IP or DNS name as the primary origin, not the application vanity domain. The vanity domain can remain the origin host header when ingress host rules require it.

## Failover Strategy

Azure Front Door health probes call `/api/health`. If AKS or the AKS ingress becomes unhealthy, Front Door marks the primary origin unavailable and shifts traffic to the secondary origin.

The secondary origin should expose the same HTTP surface:

- `/` routes to the frontend.
- `/api` routes to the API service.
- `/api/health` returns 200 when the API process is alive.
- `/api/ready` returns 200 only when PostgreSQL and Kafka are reachable.

Database recovery should use one of these patterns:

- Managed PostgreSQL read replica promoted in the secondary region/provider.
- Scheduled backups restored into the secondary environment.
- Logical replication for lower RPO workloads.

Kafka recovery should use one of these patterns:

- MirrorMaker 2 or provider-native topic replication for low RPO workloads.
- Topic recreation plus replay from durable application storage for less critical workloads.
- Backup and restore of broker volumes only when the Kafka deployment and storage class support it.

Before declaring failover complete:

1. Confirm Front Door origin health is green for the secondary origin.
2. Confirm `/api/ready` returns 200 in the secondary environment.
3. Confirm Kafka consumers are processing messages.
4. Confirm API error-rate and latency alerts are quiet.

## Failback Strategy

Failback should be gradual. Do not move all traffic back to AKS immediately after the cluster starts responding.

1. Validate AKS node, pod, ingress, and Front Door origin health.
2. Confirm `/api/health` and `/api/ready` both return 200 through the AKS ingress.
3. Sync PostgreSQL data from the active secondary database back to the AKS primary database.
4. Reconcile Kafka topics, offsets, and pending messages.
5. Shift a small percentage of traffic back to AKS.
6. Monitor API 5xx rate, p95 latency, pod restarts, and Kafka consumer errors.
7. Increase traffic gradually until AKS is primary again.

Keep the secondary origin warm until the post-failback monitoring window is complete.
