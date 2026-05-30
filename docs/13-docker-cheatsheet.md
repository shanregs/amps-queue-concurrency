# Docker Cheatsheet — amps-queue-concurrency

All commands assume you are in the **project root** (`amps-queue-concurrency/`).
Compose file is at `docker/docker-compose.yml`.  Project name: `amps-queue-concurrency`.

---

## 1. Container / Service Reference

| Container name | Service key | Host port | Role |
|---|---|---|---|
| `amps-postgres` | `postgres` | **5433** | Shared DB (multi-JVM profile) |
| `amps-publisher` | `app-publisher` | **8082** | Spring Boot publisher |
| `amps-single-subscriber` | `app-single` | **8084** | Single-subscriber profile |
| `amps-multi-subscriber` | `app-multi` | **8081** | Multi-subscriber profile |
| `amps-multi-jvm-subscriber` | `app-multi-jvm` | **8083** | Multi-JVM subscriber (PostgreSQL) |
| `amps-prometheus` | `prometheus` | **9090** | Metrics scraper |
| `amps-grafana` | `grafana` | **3000** | Dashboards (admin / admin) |
| `amps-loki` | `loki` | **3100** | Log aggregation |
| `amps-promtail` | `promtail` | *(none)* | Log shipper (Docker socket) |

---

## 2. Build

```bash
# Build all app images
docker compose -f docker/docker-compose.yml build

# Build specific services only
docker compose -f docker/docker-compose.yml build app-publisher app-multi-jvm

# Build with no cache (force fresh layer)
docker compose -f docker/docker-compose.yml build --no-cache app-publisher
```

**Expected output:**
```
[+] Building 42.3s (14/14) FINISHED
 => [app-publisher internal] load build definition from Dockerfile
 => [app-publisher] FROM eclipse-temurin:21-jre-alpine
 => [app-publisher] COPY target/*.jar app.jar
 => [app-publisher] exporting to image
 => => naming to docker.io/library/amps-queue-concurrency-app-publisher
```

---

## 3. Start / Stop

### Start the typical dev stack (publisher + multi-jvm + monitoring)
```bash
docker compose -f docker/docker-compose.yml up -d \
  postgres prometheus grafana loki promtail app-publisher app-multi-jvm
```

### Start only monitoring stack
```bash
docker compose -f docker/docker-compose.yml up -d prometheus grafana loki promtail
```

### Start a single service
```bash
docker compose -f docker/docker-compose.yml up -d app-publisher
```

### Stop all services (keep volumes)
```bash
docker compose -f docker/docker-compose.yml down
```

### Stop and wipe all data volumes
```bash
docker compose -f docker/docker-compose.yml down -v
```

**Expected output for `up -d`:**
```
[+] Running 7/7
 ✔ Container amps-postgres            Started    0.4s
 ✔ Container amps-loki                Started    0.5s
 ✔ Container amps-prometheus          Started    0.5s
 ✔ Container amps-promtail            Started    0.8s
 ✔ Container amps-grafana             Started    1.1s
 ✔ Container amps-publisher           Started    1.4s
 ✔ Container amps-multi-jvm-subscriber Started   2.2s
```

---

## 4. Status Checks

### List running containers (this project only)
```bash
docker compose -f docker/docker-compose.yml ps
```

**Expected output (healthy stack):**
```
NAME                        IMAGE                                SERVICE        STATUS          PORTS
amps-grafana                grafana/grafana:10.4.0               grafana        running         0.0.0.0:3000->3000/tcp
amps-loki                   grafana/loki:2.9.5                   loki           running         0.0.0.0:3100->3100/tcp
amps-multi-jvm-subscriber   amps-queue-concurrency-app-multi-jvm app-multi-jvm  running         0.0.0.0:8083->8080/tcp
amps-postgres               postgres:16-alpine                   postgres       running (healthy) 0.0.0.0:5433->5432/tcp
amps-prometheus             prom/prometheus:v2.51.0              prometheus     running         0.0.0.0:9090->9090/tcp
amps-promtail               grafana/promtail:2.9.5               promtail       running
amps-publisher              amps-queue-concurrency-app-publisher app-publisher  running         0.0.0.0:8082->8080/tcp
```

> **Red flag:** Any service showing `Restarting` or `Exit 1` needs investigation — check logs immediately.

### Check health of a specific container
```bash
docker inspect --format='{{.State.Health.Status}}' amps-postgres
```

**Expected output:** `healthy`

### List all images used by this project
```bash
docker compose -f docker/docker-compose.yml images
```

---

## 5. Logs

### Tail logs for one service
```bash
docker compose -f docker/docker-compose.yml logs -f app-publisher
docker compose -f docker/docker-compose.yml logs -f app-multi-jvm
```

### Tail logs for multiple services simultaneously
```bash
docker compose -f docker/docker-compose.yml logs -f app-publisher app-multi-jvm
```

### Last 50 lines then follow
```bash
docker compose -f docker/docker-compose.yml logs --tail=50 -f app-publisher
```

### Show logs with timestamps
```bash
docker compose -f docker/docker-compose.yml logs -t app-publisher
```

### Logs since a point in time
```bash
docker logs --since 5m amps-publisher
docker logs --since 2026-05-30T10:00:00 amps-publisher
```

**Expected healthy startup log (Spring Boot):**
```
amps-publisher  | {"@timestamp":"2026-05-30T10:00:01.234Z","level":"INFO",
  "message":"Started AmpsQueueConcurrencyApplication in 4.231 seconds",
  "logger":"c.s.m.a.a.AmpsQueueConcurrencyApplication"}
amps-publisher  | {"@timestamp":"2026-05-30T10:00:01.500Z","level":"INFO",
  "message":"Publisher started: RATE_LIMITED mode, 500 msg/s, 5 workers",
  "logger":"c.s.m.a.a.publisher.AmpsMessagePublisher"}
```

**Red flag patterns to grep for:**
```bash
docker logs amps-publisher 2>&1 | grep -E "ERROR|WARN|Exception|refused|timeout"
docker logs amps-multi-jvm-subscriber 2>&1 | grep -E "ERROR|FAIL|retry"
```

---

## 6. Exec Into a Container

```bash
# Shell into the publisher container
docker exec -it amps-publisher sh

# Shell into postgres and run psql
docker exec -it amps-postgres psql -U amps_user -d ampsdb

# Run a one-shot command
docker exec amps-postgres psql -U amps_user -d ampsdb -c "\dt"
```

**Expected psql table listing:**
```
             List of relations
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+-----------
 public | processed_messages| table | amps_user
(1 row)
```

---

## 7. PostgreSQL Queries (via docker exec)

```bash
# Row count — how many messages processed
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT COUNT(*) FROM processed_messages;"

# Status breakdown
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT status, COUNT(*) FROM processed_messages GROUP BY status ORDER BY status;"

# Recent failures
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT message_id, retry_count, updated_at FROM processed_messages
      WHERE status='FAILED' ORDER BY updated_at DESC LIMIT 20;"

# Discarded messages
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT message_id, retry_count FROM processed_messages WHERE status='DISCARDED';"

# Throughput per minute (last 10 min)
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT date_trunc('minute', created_at) AS minute, COUNT(*)
      FROM processed_messages
      WHERE created_at > NOW() - INTERVAL '10 minutes'
      GROUP BY 1 ORDER BY 1;"
```

**Expected status breakdown output:**
```
   status    | count
-------------+-------
 DISCARDED   |     2
 FAILED      |    15
 PROCESSED   |  9983
(3 rows)
```

---

## 8. Resource Usage

```bash
# Live CPU / memory for all project containers
docker stats amps-publisher amps-multi-jvm-subscriber amps-postgres \
             amps-prometheus amps-grafana amps-loki amps-promtail

# One-shot snapshot (no live stream)
docker stats --no-stream
```

**Expected output:**
```
CONTAINER ID   NAME                        CPU %   MEM USAGE / LIMIT     MEM %   NET I/O
a1b2c3d4e5f6   amps-publisher              12.4%   256MiB / 1GiB         25.0%   45MB / 12MB
b2c3d4e5f6a1   amps-multi-jvm-subscriber   18.7%   312MiB / 1GiB         30.4%   82MB / 34MB
c3d4e5f6a1b2   amps-postgres               2.1%    64MiB / 512MiB        12.5%   18MB / 22MB
```

> **Red flag:** App container memory > 800 MiB or CPU consistently > 80% suggests backpressure or a leak.

---

## 9. Network Inspection

```bash
# List project networks
docker network ls | grep amps

# Inspect the default bridge — confirm all containers see each other
docker network inspect amps-queue-concurrency_default

# Test connectivity from publisher to postgres (DNS-based)
docker exec amps-publisher sh -c "nc -zv postgres 5432 2>&1"

# Test publisher can reach AMPS server (external)
docker exec amps-publisher sh -c "nc -zv 172.21.12.69 9007 2>&1"
```

**Expected connectivity output:**
```
Connection to postgres (172.18.0.3) 5432 port [tcp/postgresql] succeeded!
Connection to 172.21.12.69 9007 port [tcp] succeeded!
```

---

## 10. Volume Management

```bash
# List project volumes
docker volume ls | grep amps

# Inspect a volume (find its mount path)
docker volume inspect amps-queue-concurrency_postgres-data

# Wipe only postgres data (force schema re-init on next start)
docker compose -f docker/docker-compose.yml stop postgres
docker volume rm amps-queue-concurrency_postgres-data
docker compose -f docker/docker-compose.yml up -d postgres
```

---

## 11. Restart / Recreate

```bash
# Restart one service (keep image, keep volumes)
docker compose -f docker/docker-compose.yml restart app-publisher

# Force recreate (picks up env changes, does NOT rebuild image)
docker compose -f docker/docker-compose.yml up -d --force-recreate app-publisher

# Rebuild image + recreate container
docker compose -f docker/docker-compose.yml up -d --build app-publisher
```

---

## 12. Spring Boot Actuator (from host)

Each Spring Boot container exposes Actuator on its host port.

| Endpoint | Publisher (8082) | Multi-JVM (8083) | Single (8084) | Multi (8081) |
|---|---|---|---|---|
| Health | `localhost:8082/actuator/health` | `localhost:8083/actuator/health` | `localhost:8084/actuator/health` | `localhost:8081/actuator/health` |
| Metrics list | `localhost:8082/actuator/metrics` | `localhost:8083/actuator/metrics` | — | — |
| Prometheus scrape | `localhost:8082/actuator/prometheus` | `localhost:8083/actuator/prometheus` | — | — |
| Info | `localhost:8082/actuator/info` | `localhost:8083/actuator/info` | — | — |

```bash
# Quick health check for all app containers
curl -s http://localhost:8082/actuator/health | python -m json.tool
curl -s http://localhost:8083/actuator/health | python -m json.tool
curl -s http://localhost:8084/actuator/health | python -m json.tool
curl -s http://localhost:8081/actuator/health | python -m json.tool
```

**Expected healthy output:**
```json
{
  "status": "UP",
  "components": {
    "amps": { "status": "UP", "details": { "connected": true } },
    "db":   { "status": "UP", "details": { "database": "PostgreSQL" } },
    "diskSpace": { "status": "UP" }
  }
}
```

> **Red flag:** `"status": "DOWN"` on `amps` component means the subscriber/publisher lost its TCP connection to `172.21.12.69:9007`.

---

## 13. Common Troubleshooting

| Symptom | Command | What to look for |
|---|---|---|
| Container keeps restarting | `docker logs --tail=30 <container>` | Stack trace, port conflict, OOM |
| AMPS connection refused | `docker exec <app> sh -c "nc -zv 172.21.12.69 9007"` | `succeeded!` vs `failed` |
| postgres not ready | `docker inspect --format='{{.State.Health.Status}}' amps-postgres` | `healthy` |
| promtail not shipping | `docker logs amps-promtail 2>&1 \| grep -i error` | JMESPath / socket errors |
| Loki not receiving | `curl http://localhost:3100/ready` | `ready` |
| High memory in app | `docker stats --no-stream amps-multi-jvm-subscriber` | MEM % > 80 |
| Publisher not publishing | `curl localhost:8082/actuator/health` | `amps.status = UP` |

---

## 14. Full Stack Teardown and Clean Restart

```bash
# Stop everything and remove containers + volumes
docker compose -f docker/docker-compose.yml down -v

# Remove dangling build cache
docker builder prune -f

# Rebuild and start fresh
docker compose -f docker/docker-compose.yml build app-publisher app-multi-jvm
docker compose -f docker/docker-compose.yml up -d \
  postgres prometheus grafana loki promtail app-publisher app-multi-jvm

# Confirm all healthy
docker compose -f docker/docker-compose.yml ps
```
