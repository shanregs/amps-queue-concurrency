# Observability Cheatsheet — Prometheus · Grafana · Loki

Reference for monitoring, metrics, and log queries against the running
`amps-queue-concurrency` stack.

| Tool | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | none |
| Loki (raw API) | http://localhost:3100 | none |
| Promtail status | http://localhost:9080 | none |

---

## 1. Architecture Overview

```
Spring Boot Apps (/actuator/prometheus)
         │  scrape every 15s
         ▼
    Prometheus  ──────────────────────────────────────┐
         │                                            │
         │  PromQL queries                            │  data source
         ▼                                            ▼
      Grafana  ◄───────────────────────────────── Loki
         ▲                                            ▲
         │  dashboards                                │  push logs (HTTP)
         │                                            │
    (browser)                                    Promtail
                                                      ▲
                                          reads Docker socket
                                          filters label: logging=promtail
                                          parses JSON log lines
                                          extracts: level, service,
                                                    spring_profile,
                                                    messageId, topic,
                                                    subscriberIndex,
                                                    publisherWorker
```

---

## 2. Prometheus

### 2.1 Browser UI — key pages

| Page | URL | Use |
|---|---|---|
| Targets | http://localhost:9090/targets | Are all Spring Boot apps being scraped? |
| Graph / Explorer | http://localhost:9090/graph | Run PromQL queries |
| Config | http://localhost:9090/config | Verify scrape jobs loaded |
| Alerts | http://localhost:9090/alerts | Rule evaluation status |

### 2.2 Verify scrape targets are UP

Open http://localhost:9090/targets — you should see:

```
job="amps-queue-app"
  app-single:8080     UP    last scrape 3.2s ago   duration 45ms
  app-multi:8080      UP    last scrape 3.5s ago   duration 38ms
  app-publisher:8080  UP    last scrape 2.8s ago   duration 41ms
  app-multi-jvm:8080  UP    last scrape 4.1s ago   duration 52ms

job="prometheus"
  localhost:9090      UP    last scrape 0.1s ago   duration 3ms
```

> **Red flag:** `DOWN` on any app target → check `docker compose ps` and Actuator health.

### 2.3 Prometheus API — check health

```bash
# Is Prometheus itself healthy?
curl http://localhost:9090/-/healthy
# Expected: Prometheus Server is Healthy.

# Is config loaded?
curl http://localhost:9090/-/ready
# Expected: Prometheus Server is Ready.

# Reload config without restart
curl -X POST http://localhost:9090/-/reload
```

### 2.4 Key PromQL queries

Run these in http://localhost:9090/graph or paste into a Grafana panel.

#### JVM and process

```promql
# JVM heap used (MB) — per app instance
sum(jvm_memory_used_bytes{area="heap"}) by (instance) / 1024 / 1024

# Live thread count (virtual threads show here too)
jvm_threads_live_threads

# GC pause time rate (seconds per second)
rate(jvm_gc_pause_seconds_sum[1m])

# CPU usage
process_cpu_usage * 100
```

#### HTTP / Actuator

```promql
# Request rate (req/s) per endpoint
rate(http_server_requests_seconds_count[1m])

# 99th-percentile latency
histogram_quantile(0.99, rate(http_server_requests_seconds_bucket[5m]))

# Error rate (4xx + 5xx)
sum(rate(http_server_requests_seconds_count{status=~"4..|5.."}[1m])) by (uri)
```

#### AMPS message processing (custom Micrometer metrics)

These are produced by `MessageProcessor` and `PublisherMetrics`.

```promql
# Messages processed per second (all statuses)
rate(amps_messages_processed_total[1m])

# Processed vs failed breakdown
sum(rate(amps_messages_processed_total[1m])) by (status)

# Publisher messages sent per second
rate(amps_publisher_messages_sent_total[1m])

# Retry count distribution
histogram_quantile(0.95, rate(amps_message_retry_count_bucket[5m]))

# Semaphore queue depth (backpressure indicator)
amps_semaphore_queue_depth

# Active virtual thread workers
amps_active_vt_workers
```

> **Note:** If a metric returns `no data`, it may not have fired yet — publish some messages first.

#### Database / HikariCP

```promql
# Active DB connections
hikaricp_connections_active

# Pending connection acquisition (> 0 = pool pressure)
hikaricp_connections_pending

# Connection acquisition time (p99)
histogram_quantile(0.99,
  rate(hikaricp_connections_acquisition_seconds_bucket[5m]))
```

#### Expected healthy ranges

| Metric | Healthy | Concern |
|---|---|---|
| `jvm_memory_used_bytes{area="heap"}` | < 600 MB | > 900 MB |
| `hikaricp_connections_pending` | 0 | > 2 sustained |
| `process_cpu_usage` | < 0.7 | > 0.9 sustained |
| `amps_messages_processed_total{status="FAILED"}` rate | near 0 | > 1% of total |
| `amps_semaphore_queue_depth` | 0 | > 10 sustained |

---

## 3. Grafana

### 3.1 First-time login

1. Open http://localhost:3000
2. Login: `admin` / `admin`  (skip password change prompt or set a new one)
3. Left sidebar → **Connections → Data sources** — verify:
   - `Prometheus` → http://prometheus:9090 → **Save & Test** → green tick
   - `Loki` → http://loki:3100 → **Save & Test** → green tick

### 3.2 Explore view — ad-hoc queries

**Metrics (Prometheus):**
1. Left sidebar → **Explore** (compass icon)
2. Top-left dropdown → select **Prometheus**
3. Use **Metrics browser** or type PromQL directly
4. Hit **Run query** (Shift+Enter)

**Logs (Loki):**
1. Left sidebar → **Explore**
2. Top-left dropdown → select **Loki**
3. Use **Label browser** or type LogQL directly

### 3.3 Create a dashboard — quick steps

1. Left sidebar → **Dashboards → New → New dashboard**
2. **Add visualization** → choose Prometheus or Loki data source
3. Paste a PromQL / LogQL query
4. Set panel title, visualization type (Time series / Stat / Table / Logs)
5. **Save** (Ctrl+S) → name: `AMPS Queue Concurrency`

### 3.4 Useful Grafana variables for dashboards

Add these as **dashboard variables** (Dashboard settings → Variables):

| Variable | Type | Query | Use |
|---|---|---|---|
| `$instance` | Query | `label_values(jvm_info, instance)` | Filter by app instance |
| `$service` | Query | `label_values({job="amps-queue-app"}, service)` | Filter Loki by service |
| `$level` | Custom | `INFO,WARN,ERROR` | Filter log level |

### 3.5 Suggested panels

| Panel | Type | Query |
|---|---|---|
| Messages processed/s | Time series | `rate(amps_messages_processed_total[1m])` |
| Fail rate % | Stat | `100 * rate(amps_messages_processed_total{status="FAILED"}[5m]) / rate(amps_messages_processed_total[5m])` |
| JVM heap | Time series | `jvm_memory_used_bytes{area="heap"} / 1024 / 1024` |
| Active DB connections | Stat | `hikaricp_connections_active` |
| Log stream | Logs panel | `{service="app-multi-jvm", level="ERROR"}` |
| Publisher send rate | Time series | `rate(amps_publisher_messages_sent_total[1m])` |

---

## 4. Loki — Log Queries (LogQL)

### 4.1 Labels available (set by Promtail)

| Label | Values | Source |
|---|---|---|
| `service` | `app-publisher`, `app-single`, `app-multi`, `app-multi-jvm`, `postgres`, `prometheus`, `loki` | Docker Compose service name |
| `container` | `amps-publisher`, `amps-multi-jvm-subscriber`, … | Docker container name |
| `level` | `INFO`, `WARN`, `ERROR`, `DEBUG` | Extracted from JSON log line |
| `spring_profile` | `message-publisher`, `multi-jvm-subscriber`, `single-subscriber`, `multi-subscriber` | Docker label |

### 4.2 Basic log stream queries

```logql
# All logs from the publisher
{service="app-publisher"}

# All ERROR logs across all app containers
{service=~"app-.*", level="ERROR"}

# WARN and ERROR logs from multi-jvm subscriber
{service="app-multi-jvm", level=~"WARN|ERROR"}

# Logs containing the word "retry"
{service=~"app-.*"} |= "retry"

# Logs NOT containing health-check noise
{service="app-publisher"} != "/actuator/health"
```

### 4.3 JSON field extraction (parse log content)

Promtail already promotes `level`, `service`, and `spring_profile` to labels.
For other fields (`messageId`, `topic`, `subscriberIndex`, `publisherWorker`)
use inline parsing in LogQL:

```logql
# Show messageId and topic for all FAILED processing attempts
{service="app-multi-jvm", level="ERROR"}
  | json messageId="messageId", topic="topic"
  | line_format "{{.messageId}} on {{.topic}}"

# Subscriber-specific errors (which subscriber index is struggling)
{service="app-multi-jvm", level="ERROR"}
  | json subscriberIndex="subscriberIndex"
  | keep subscriberIndex, __line__

# Publisher worker output
{service="app-publisher"}
  | json publisherWorker="publisherWorker"
  | publisherWorker != ""
```

### 4.4 Log aggregation queries (metrics from logs)

```logql
# Error rate per minute (count over time)
sum(count_over_time({service=~"app-.*", level="ERROR"}[1m])) by (service)

# Log volume per service per minute
sum(rate({service=~"app-.*"}[1m])) by (service)

# Retry message count (lines containing "Retrying") per minute
count_over_time({service="app-multi-jvm"} |= "Retrying" [1m])
```

### 4.5 Loki HTTP API — quick checks

```bash
# Is Loki ready to accept queries?
curl http://localhost:3100/ready
# Expected: ready

# Is Loki healthy?
curl http://localhost:3100/loki/api/v1/status/buildinfo
# Expected: JSON with version, revision, goVersion

# Query last 10 ERROR log lines via API
curl -G http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service="app-multi-jvm",level="ERROR"}' \
  --data-urlencode 'limit=10' \
  --data-urlencode "start=$(date -d '5 minutes ago' +%s)000000000" \
  --data-urlencode "end=$(date +%s)000000000" | python -m json.tool

# List all label names
curl http://localhost:3100/loki/api/v1/labels
# Expected: {"status":"200","data":["container","level","service","spring_profile",...]}

# List values for a label
curl "http://localhost:3100/loki/api/v1/label/service/values"
# Expected: {"status":"200","data":["app-multi-jvm","app-publisher","loki","prometheus",...]}
```

---

## 5. Promtail — Verify Log Shipping

### 5.1 Check Promtail is running

```bash
docker logs amps-promtail --tail=20
```

**Expected healthy output:**
```
level=info msg="Starting Promtail" version="2.9.5"
level=info msg="Watching Docker containers" filters="[{Name:label Values:[logging=promtail]}]"
level=info msg="tail routine: started" path="/var/lib/docker/containers/abc123.../abc123...-json.log"
```

**Red flag patterns:**
```
level=error msg="error parsing pipeline stage" ...   ← JSON/JMESPath config error
level=error msg="permission denied"                  ← Docker socket not mounted
level=error msg="connection refused" url=http://loki:3100  ← Loki not reachable
```

### 5.2 Promtail targets page

Open http://localhost:9080/targets — lists all Docker containers Promtail is tailing.

**Expected:** One entry per container that has label `logging=promtail` in docker-compose.yml.

```
Job: docker-containers
  /var/lib/docker/containers/abc.../abc...-json.log  → labels: {container="amps-publisher", service="app-publisher", spring_profile="message-publisher", level="INFO"}  State: active
  /var/lib/docker/containers/def.../def...-json.log  → labels: {container="amps-multi-jvm-subscriber", service="app-multi-jvm", ...}  State: active
```

### 5.3 End-to-end log pipeline verification

1. Generate a log line in the publisher:
   ```bash
   curl http://localhost:8082/actuator/health  # triggers INFO log
   ```
2. Check Promtail picked it up:
   ```bash
   docker logs amps-promtail --tail=5 | grep "sent"
   ```
3. Check Loki received it (Grafana Explore → Loki → `{service="app-publisher"}`).

---

## 6. Quick Observability Checklist

Run this sequence after starting the stack to confirm everything is wired up.

```bash
# 1. All containers running?
docker compose -f docker/docker-compose.yml ps

# 2. Prometheus healthy and scraping?
curl -s http://localhost:9090/-/healthy
curl -s http://localhost:9090/api/v1/targets | python -m json.tool | grep '"health"'
# Expected: all "health": "up"

# 3. Loki ready?
curl -s http://localhost:3100/ready

# 4. Grafana reachable?
curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health
# Expected: 200

# 5. App metrics flowing?
curl -s http://localhost:8082/actuator/prometheus | grep "jvm_memory_used_bytes"
# Expected: lines with numeric values

# 6. App logs in Loki?
curl -s "http://localhost:3100/loki/api/v1/label/service/values"
# Expected JSON contains "app-publisher"

# 7. Postgres processed messages?
docker exec amps-postgres psql -U amps_user -d ampsdb \
  -c "SELECT status, COUNT(*) FROM processed_messages GROUP BY status;"
```

---

## 7. Troubleshooting

| Symptom | Check | Fix |
|---|---|---|
| Prometheus target DOWN | `docker compose ps` → is app running? Actuator health? | Restart app; check AMPS connection |
| No metrics in Grafana | Grafana → Connections → Prometheus → Test | Verify URL `http://prometheus:9090` (not localhost) |
| Loki data source error | Grafana → Connections → Loki → Test | Verify URL `http://loki:3100` (not localhost) |
| Logs not appearing in Loki | `docker logs amps-promtail` for errors | Check Docker socket mount; check `logging=promtail` label on service |
| Log lines show as plain text (not parsed) | Query `{service="app-publisher"}` — see raw JSON? | Confirm `docker-logging` Spring profile active |
| `level` label missing on logs | Check Promtail pipeline stages config | `level` must be extracted in JSON stage and promoted in labels stage |
| Old log data not found | Loki default retention: 7 days | Adjust `max_query_lookback` in `loki-config.yaml` |
| Grafana dashboard blank after import | Check variable `$instance` resolves | Prometheus must have scraped at least once |

---

## 8. Retention and Data Limits

| Setting | Value | Location |
|---|---|---|
| Prometheus retention | 7 days | `docker-compose.yml` `--storage.tsdb.retention.time=7d` |
| Loki max query lookback | 7 days (168h) | `docker/loki/loki-config.yaml` `max_query_lookback` |
| Loki ingestion rate | 8 MB/s burst 16 MB/s | `docker/loki/loki-config.yaml` |
| Prometheus scrape interval | 15s | `docker/prometheus/prometheus.yml` |
| Promtail Docker poll interval | 5s | `docker/promtail/promtail-config.yaml` |
