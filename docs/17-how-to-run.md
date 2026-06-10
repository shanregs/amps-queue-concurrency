# 17 — How to Run: Publisher + Multi-JVM Subscribers

**Scenario:** 1 publisher JVM publishes simulated trade messages to AMPS.
Two independent subscriber JVMs compete for messages from the same queue, sharing
PostgreSQL for cross-JVM idempotency.

```
[Publisher JVM]  ──publish──▶  AMPS /queue/trades  ──deliver──▶  [Subscriber JVM 1]
                               (172.21.12.69:9007)               [Subscriber JVM 2]
                                                                         │
                                                               shared PostgreSQL
                                                            (cross-JVM idempotency)
```

---

## Table of Contents

1. [Environment & Prerequisites](#1-environment--prerequisites)
2. [Option A — Docker (Recommended)](#2-option-a--docker-recommended)
3. [Option B — Windows Terminal](#3-option-b--windows-terminal)
4. [Option C — IntelliJ IDEA](#4-option-c--intellij-idea)
5. [Observing AMPS](#5-observing-amps)
6. [Observing with Prometheus](#6-observing-with-prometheus)
7. [Observing with Grafana](#7-observing-with-grafana)
8. [Quick Shutdown](#8-quick-shutdown)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Environment & Prerequisites

### Infrastructure

| Component    | Location                        | Notes                                   |
|--------------|---------------------------------|-----------------------------------------|
| AMPS Server  | WSL2 @ `172.21.12.69`          | TCP client: `:9007`, Admin: `:8085`     |
| PostgreSQL   | Docker container                | Required for multi-JVM idempotency      |
| Prometheus   | `localhost:9090`                | Started with Docker monitoring stack    |
| Grafana      | `localhost:3000`                | `admin` / `admin`                       |
| Loki         | `localhost:3100`                | Log aggregation via Promtail            |

> **WSL2 IP note:** `172.21.12.69` can change after a reboot. Check with
> `wsl hostname -I` and update `amps.server.uri` in `application.yaml` or set
> `AMPS_SERVER_URI` at runtime.

### Software requirements

| Tool           | Minimum | Check                      |
|----------------|---------|----------------------------|
| Java (Temurin) | 21      | `java -version`            |
| Maven Wrapper  | bundled | `.\mvnw.cmd --version`     |
| Docker Desktop | 24+     | `docker --version`         |
| Docker Compose | v2      | `docker compose version`   |
| IntelliJ IDEA  | 2023.2+ | Option C only              |

### Step 0 — Verify AMPS is reachable

```powershell
Test-NetConnection -ComputerName 172.21.12.69 -Port 9007   # expected: TcpTestSucceeded: True
Test-NetConnection -ComputerName 172.21.12.69 -Port 8085   # expected: TcpTestSucceeded: True
curl http://172.21.12.69:8085/amps/admin                   # expected: AMPS HTML/JSON response
```

---

## 2. Option A — Docker (Recommended)

All three JVMs run as containers. PostgreSQL, Prometheus, Grafana, and Loki are also
containers in the same Compose project (`amps-queue-concurrency`).

### Step 1 — Build the JAR and Docker images

Run once, or after any code change:

```powershell
# From project root
.\mvnw.cmd clean package -DskipTests

docker compose -f docker/docker-compose.yml build app-publisher app-multi-jvm
```

### Step 2 — Start the full stack (infra + publisher + subscriber JVM 1)

```powershell
docker compose -f docker/docker-compose.yml up -d `
  postgres prometheus grafana loki promtail `
  app-publisher app-multi-jvm
```

Wait for PostgreSQL health gate — Compose holds `app-multi-jvm` start until postgres is healthy.

**Verify both are up:**
```powershell
docker compose -f docker/docker-compose.yml ps
# STATUS must show: Up (healthy) for postgres, Up for app-publisher and app-multi-jvm
```

**Check startup logs:**
```powershell
# Publisher — look for: "AmpsMessagePublisher started: workers=5 topic=/queue/trades mode=RATE_LIMITED runDuration=30m"
docker compose -f docker/docker-compose.yml logs app-publisher --tail=10
# To override run-duration, set AMPS_PUBLISHER_RUN_DURATION before the compose command:
#   $env:AMPS_PUBLISHER_RUN_DURATION = "5m"; docker compose ... up -d app-publisher

# Subscriber JVM 1 — look for: "MultiAmpsSubscriberPool : Starting 3 AMPS subscribers"
docker compose -f docker/docker-compose.yml logs app-multi-jvm --tail=10
```

**Actuator health:**
```powershell
curl http://localhost:8082/actuator/health   # Publisher  → amps:UP, db:UP (H2)
curl http://localhost:8083/actuator/health   # Sub JVM 1  → amps:UP, db:UP (PostgreSQL)
```

### Step 3 — Start subscriber JVM 2

JVM 2 is started with `docker run` because the Compose file binds `app-multi-jvm` to port
`8083`. JVM 2 uses port `8093`. Both share the same Docker network and PostgreSQL.

```powershell
docker run -d `
  --name amps-multi-jvm-sub2 `
  --network amps-queue-concurrency_default `
  -p 8093:8080 `
  -e SPRING_PROFILES_ACTIVE="multi-jvm-subscriber,docker-logging" `
  -e AMPS_SERVER_URI="tcp://172.21.12.69:9007/amps/json" `
  -e DB_HOST=postgres `
  -e DB_USER=amps_user `
  -e DB_PASSWORD=changeme `
  -e AMPS_CONSUMER_SUBSCRIBER_COUNT=3 `
  -e AMPS_CONSUMER_MAX_CONCURRENCY_PER_SUBSCRIBER=50 `
  -v amps-queue-concurrency_bookmark-multi-jvm-sub2:/home/amps/.amps/bookmarks `
  --label logging=promtail `
  --label spring_profile=multi-jvm-subscriber `
  amps-queue-concurrency-app-multi-jvm
```

**Verify JVM 2:**
```powershell
docker logs amps-multi-jvm-sub2 --tail=10
curl http://localhost:8093/actuator/health   # → amps:UP, db:UP (PostgreSQL)
```

### Step 4 — Verify competing consumption in PostgreSQL

```powershell
# Per-JVM message split (container ID = processed_by)
docker exec amps-postgres psql -U amps_user -d ampsdb `
  -c "SELECT processed_by, status, COUNT(*) FROM processed_messages GROUP BY processed_by, status ORDER BY processed_by;"

# Idempotency check — duplicates must be 0
docker exec amps-postgres psql -U amps_user -d ampsdb `
  -c "SELECT COUNT(*) total, COUNT(DISTINCT message_id) unique_ids, COUNT(*)-COUNT(DISTINCT message_id) duplicates FROM processed_messages;"
```

### Step 5 — After a code change: rebuild and restart

```powershell
# 1. Build new JAR
.\mvnw.cmd clean package -DskipTests

# 2. Rebuild image
docker compose -f docker/docker-compose.yml build app-multi-jvm

# 3. Restart JVM 1
docker compose -f docker/docker-compose.yml up -d --force-recreate app-multi-jvm

# 4. Replace JVM 2
docker stop amps-multi-jvm-sub2; docker rm amps-multi-jvm-sub2
# Then re-run the docker run command from Step 3
```

> After restarting, the monitoring stack (`prometheus grafana loki promtail`) must also be
> restarted if the Docker network was recreated (this happens when the whole Compose project
> is recreated). Run: `docker compose -f docker/docker-compose.yml up -d prometheus grafana loki promtail`

---

## 3. Option B — Windows Terminal

Run all three processes natively on Windows. PostgreSQL still runs in Docker.

### Step 1 — Build the JAR

```powershell
.\mvnw.cmd clean package -DskipTests
# Output: target/amps-queue-concurrency-0.0.1-SNAPSHOT.jar
```

### Step 2 — Start PostgreSQL (Docker only)

```powershell
docker compose -f docker/docker-compose.yml up -d postgres

# Wait for healthy (check STATUS column)
docker compose -f docker/docker-compose.yml ps postgres
```

### Step 3 — Start monitoring stack (optional but recommended)

```powershell
docker compose -f docker/docker-compose.yml up -d prometheus grafana loki promtail
```

> For Windows Terminal mode, update `docker/prometheus/prometheus.yml` to scrape
> `host.docker.internal:8082`, `host.docker.internal:8083`, `host.docker.internal:8093`
> instead of the Docker service names.

### Step 4 — Open 3 PowerShell tabs in Windows Terminal

Use **Ctrl+Shift+T** to open three tabs. Run one command per tab, in order.

**Tab 1 — Publisher (port 8082)**

```powershell
$env:AMPS_SERVER_URI             = "tcp://172.21.12.69:9007/amps/json"
$env:AMPS_PUBLISHER_RUN_DURATION = "30m"   # optional: 30s | 5m | 2h

.\mvnw.cmd spring-boot:run `
  "-Dspring-boot.run.profiles=message-publisher" `
  "-Dspring-boot.run.jvmArguments=-Dserver.port=8082"
```

Wait for: `AmpsMessagePublisher started: workers=5 topic=/queue/trades mode=RATE_LIMITED`

**Tab 2 — Subscriber JVM 1 (port 8083)**

```powershell
$env:AMPS_SERVER_URI        = "tcp://172.21.12.69:9007/amps/json"
$env:SPRING_DATASOURCE_URL  = "jdbc:postgresql://localhost:5433/ampsdb"
$env:DB_USER                = "amps_user"
$env:DB_PASSWORD            = "changeme"

.\mvnw.cmd spring-boot:run `
  "-Dspring-boot.run.profiles=multi-jvm-subscriber" `
  "-Dspring-boot.run.jvmArguments=-Dserver.port=8083"
```

Wait for: `MultiAmpsSubscriberPool : Starting 3 AMPS subscribers`

**Tab 3 — Subscriber JVM 2 (port 8093)**

Uses a different `server.port` to avoid binding conflict with JVM 1.
Connects to the same AMPS topic and same PostgreSQL — AMPS delivers each message
to exactly one subscriber (competing consumer model).

```powershell
$env:AMPS_SERVER_URI        = "tcp://172.21.12.69:9007/amps/json"
$env:SPRING_DATASOURCE_URL  = "jdbc:postgresql://localhost:5433/ampsdb"
$env:DB_USER                = "amps_user"
$env:DB_PASSWORD            = "changeme"

.\mvnw.cmd spring-boot:run `
  "-Dspring-boot.run.profiles=multi-jvm-subscriber" `
  "-Dspring-boot.run.jvmArguments=-Dserver.port=8093"
```

### Step 5 — Verify all three processes

```powershell
curl http://localhost:8082/actuator/health   # Publisher
curl http://localhost:8083/actuator/health   # Sub JVM 1
curl http://localhost:8093/actuator/health   # Sub JVM 2

# Per-JVM split in PostgreSQL (processed_by = hostname)
docker exec amps-postgres psql -U amps_user -d ampsdb `
  -c "SELECT processed_by, COUNT(*) FROM processed_messages GROUP BY processed_by;"
```

---

## 4. Option C — IntelliJ IDEA

Run all three as separate Spring Boot run configurations.

### Step 1 — Start PostgreSQL first

```powershell
docker compose -f docker/docker-compose.yml up -d postgres
```

### Step 2 — Create Publisher run configuration

1. **Run → Edit Configurations → + → Spring Boot**

   | Field                 | Value                                        |
   |-----------------------|----------------------------------------------|
   | Name                  | `AMPS Publisher`                             |
   | Main class            | `AmpsQueueConcurrencyApplication`            |
   | Active profiles       | `message-publisher`                          |
   | VM options            | `-Dserver.port=8082`                         |
   | Environment variables | `AMPS_SERVER_URI=tcp://172.21.12.69:9007/amps/json;AMPS_PUBLISHER_RUN_DURATION=30m` |

2. Click **OK**.

### Step 3 — Create Subscriber JVM 1 run configuration

1. **Run → Edit Configurations → + → Spring Boot**

   | Field                 | Value                                                                              |
   |-----------------------|------------------------------------------------------------------------------------|
   | Name                  | `AMPS Subscriber JVM-1`                                                            |
   | Main class            | `AmpsQueueConcurrencyApplication`                                                  |
   | Active profiles       | `multi-jvm-subscriber`                                                             |
   | VM options            | `-Dserver.port=8083`                                                               |
   | Environment variables | `AMPS_SERVER_URI=tcp://172.21.12.69:9007/amps/json;SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/ampsdb;DB_USER=amps_user;DB_PASSWORD=changeme` |

2. Click **OK**.

### Step 4 — Create Subscriber JVM 2 run configuration

1. Right-click `AMPS Subscriber JVM-1` → **Copy**.
2. Rename to `AMPS Subscriber JVM-2`.
3. Change **VM options** to `-Dserver.port=8093`.
4. All environment variables are the same as JVM 1.
5. Click **OK**.

> IntelliJ runs each Spring Boot config in a separate OS process — this genuinely simulates
> two independent JVMs competing for the same AMPS queue.

### Step 5 — Run all three (Compound configuration)

1. **Run → Edit Configurations → + → Compound**
2. Name: `AMPS Full Stack`
3. Add: `AMPS Publisher`, `AMPS Subscriber JVM-1`, `AMPS Subscriber JVM-2`
4. Click **OK** → hit **▶** once to start all three simultaneously.

### Step 6 — Watch the Run tabs

IntelliJ opens a separate **Run** tab per configuration:

| Tab                    | Healthy log lines to see                          |
|------------------------|---------------------------------------------------|
| AMPS Publisher         | `Published seq=1000 total=997 errors=0`           |
| AMPS Subscriber JVM-1  | `OK messageId=... topic=/queue/trades`            |
| AMPS Subscriber JVM-2  | `OK messageId=... topic=/queue/trades`            |

Messages alternate between the two subscriber tabs — each message goes to exactly one JVM.

---

## 5. Observing AMPS

### 5.1 Admin UI

Open `http://172.21.12.69:8085/amps/admin`

| Metric              | Healthy value                         | Problem signal                            |
|---------------------|---------------------------------------|-------------------------------------------|
| `queued`            | 0 – low hundreds                      | Growing → subscribers falling behind      |
| `leased`            | > 0 while messages are flowing        | 0 = no subscribers connected              |
| `lease_expire`      | 0                                     | > 0 = processing failures, retries active |
| Client count        | 7 (1 pub + 2 JVMs × 3 subscribers)   | Missing clients = a JVM failed to connect |
| `publish_rate`      | ≈ 500/s                               | 0 = publisher not running                 |

### 5.2 AMPS REST API

```powershell
# Queue depth and rates
curl "http://172.21.12.69:8085/amps/admin/topics?topic=/queue/trades"

# Connected clients (should show 7: 1 publisher HAClient + 6 subscriber HAClients)
curl http://172.21.12.69:8085/amps/admin/clients | python -m json.tool
```

---

## 6. Observing with Prometheus

Open `http://localhost:9090`

### 6.1 Verify scrape targets

Navigate to **Status → Targets** (`http://localhost:9090/targets`).

Expected:
```
job="amps-queue-app"
  app-publisher:8080         UP
  app-multi-jvm:8080         UP   ← subscriber JVM 1
  amps-multi-jvm-sub2:8080   UP   ← subscriber JVM 2
```

`app-single:8080` and `app-multi:8080` will show DOWN if those profiles are not running — that is expected.

### 6.2 Key PromQL queries (paste into http://localhost:9090/graph)

**Publisher throughput:**
```promql
rate(amps_publisher_messages_published_total[30s])
```

**Subscriber consumed/s by result (both JVMs, all results):**
```promql
sum by (instance, result) (rate(amps_subscriber_messages_total[30s]))
```

**Per-JVM processing throughput (OK messages only):**
```promql
sum by (instance) (rate(amps_subscriber_messages_total{result="OK"}[30s]))
```

**Active virtual threads per subscriber JVM:**
```promql
amps_subscriber_active_vt_workers
```

**Processing latency p99 per JVM:**
```promql
histogram_quantile(0.99,
  sum by (instance, le) (rate(amps_subscriber_processing_duration_seconds_bucket[1m])))
```

**HikariCP pool pressure:**
```promql
hikaricp_connections_active
hikaricp_connections_pending
```

---

## 7. Observing with Grafana

Open `http://localhost:3000` — login: `admin` / `admin`

### 7.1 Pre-provisioned dashboard

Navigate to **Dashboards → AMPS Queue Concurrency**.

The dashboard has four sections:

**Publisher section**

| Panel                     | What to watch                           |
|---------------------------|-----------------------------------------|
| Messages Published (total)| Cumulative counter, grows continuously  |
| Publish Errors            | Must stay 0 (red if > 0)               |
| Active Publisher Workers  | Fixed at 5 (configured workers)        |
| Publish Rate (msg/s)      | Steady at 500/s                         |
| Publish Latency           | p99 < 50ms is healthy                   |

**Subscriber section** ← new panels

| Panel                          | What to watch                                        |
|--------------------------------|------------------------------------------------------|
| Messages Consumed/s by Result  | Two `OK` lines (one per JVM), near-zero for others  |
| Active VT Workers per JVM      | Fluctuating 0–50; flat at 50 = backpressure          |
| Processing Duration p50/p95/p99| p99 < 50ms; sudden spike = DB or GC pressure         |
| HikariCP Active Connections    | Well below 20; `pending > 0` = pool exhaustion       |

**JVM section**

| Panel                  | What to watch                             |
|------------------------|-------------------------------------------|
| Virtual Threads (live) | Spikes with message bursts                |
| JVM Heap Used          | Stable; no steady climb                   |

**Logs section**

Live log stream from all `app-*` containers filtered to `WARN` and above.

### 7.2 Verify subscriber metrics are visible

After the dashboard loads, the **Subscriber** section should show two separate lines
(one per JVM instance) on each timeseries panel. If panels show "No data":
1. Confirm Prometheus targets are UP (§6.1)
2. Wait 30s for the first scrape cycle
3. Check `http://localhost:8083/actuator/prometheus` returns `amps_subscriber_*` lines

---

## 8. Quick Shutdown

### Docker

```powershell
# Graceful stop — SmartLifecycle drains in-flight VTs before exit
docker compose -f docker/docker-compose.yml stop app-publisher app-multi-jvm
docker stop amps-multi-jvm-sub2

# Stop monitoring stack (data volumes preserved)
docker compose -f docker/docker-compose.yml stop prometheus grafana loki promtail postgres

# Full teardown including volumes (wipes Postgres data, Prometheus metrics)
docker compose -f docker/docker-compose.yml down -v
docker rm amps-multi-jvm-sub2
docker volume rm amps-queue-concurrency_bookmark-multi-jvm-sub2
```

### Windows Terminal

Press **Ctrl+C** in each tab. Spring Boot triggers `SmartLifecycle` shutdown:
1. Subscriber stops accepting new AMPS deliveries
2. Waits up to 30s for in-flight VTs to complete and ACK
3. Closes HAClient — AMPS releases all active leases

```powershell
# After Ctrl+C in all three tabs:
docker compose -f docker/docker-compose.yml stop postgres
```

### IntelliJ

Click the red ■ stop button per **Run** tab, or stop the Compound configuration.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `TcpTestSucceeded: False` on port 9007 | AMPS not on port 9007, or WSL2 IP changed | Run `wsl hostname -I` to get current IP; check AMPS `config.xml` for `<Port>` value |
| Subscriber container stays at Hibernate init | AMPS HAClient hanging on TCP connect | Verify `AMPS_SERVER_URI` port; test connectivity before starting containers |
| `BeanCreationException: DataSource` | PostgreSQL not running or wrong `DB_HOST` | `docker compose ... up -d postgres` first; check `SPRING_DATASOURCE_URL` |
| Grafana subscriber panels show "No data" | Prometheus not scraping the container | Check `http://localhost:9090/targets` — JVM must be UP; wait 30s for first scrape |
| `amps-multi-jvm-sub2` not in Prometheus | Container started after Prometheus picked up config | Prometheus auto-reloads `prometheus.yml` every scrape interval (15s); wait 15s |
| PostgreSQL `duplicates > 0` | Unique constraint missing on `message_id` | Run `docker exec amps-postgres psql ... -f docker/postgres/init.sql` to re-apply schema |
| `Port 8093 already in use` (Windows) | Another process on 8093 | Change JVM 2 to `-Dserver.port=8094` and update `prometheus.yml` accordingly |
| Network recreated after `--force-recreate` | Docker Compose rebuilds the default network | Restart monitoring stack: `docker compose up -d prometheus grafana loki promtail` |
| JVM 2 shows in Prometheus but missing from Grafana | Dashboard variable filter | Open Grafana dashboard → click Refresh, or set time range to "Last 5 minutes" |
