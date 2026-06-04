# Public Interactive Demo — Architecture & AWS Deployment

This guide describes how to run a **Kafka-free**, **read-mostly** public demo of the Wildfire Monitoring platform. The full production stack (Kafka, Airflow, workers) remains in the main `docker-compose.yml`; the demo path is intentionally minimal.

---

## Architecture comparison

### Production (local / portfolio full stack)

```
Airflow / HTTP → Kafka → ingestion → validation → consumer → PostgreSQL
                                    ↘ map-worker → Folium HTML
PostgreSQL → map-api (/map)
```

### Public demo

```
PostgreSQL → map-api (FastAPI) → static dashboard + /demo/wildfires
            ↘ /map (Folium)     ↘ /docs (Swagger)
```

Optional **offline refresh** (no Kafka):

```
NASA FIRMS CSV → validate → postgres_write → PostgreSQL
  (scripts/sync_firms_to_postgres.py or POST /demo/sync)
```

---

## Text architecture diagram (AWS)

```
                    Internet
                        │
                        ▼
              ┌─────────────────┐
              │  Route 53 (opt) │
              └────────┬────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │ Application Load Balancer     │
         │  HTTP :80 → TG :8003          │
         └──────────────┬───────────────┘
                        │
         ┌──────────────▼───────────────┐
         │ EC2 (t3.micro / t4g.micro)    │
         │  Docker: map-api container    │
         │  DEMO_MODE=true               │
         │  MAP_CONSUMER_ENABLED=false   │
         └──────────────┬───────────────┘
                        │ 5432 (private SG)
         ┌──────────────▼───────────────┐
         │ RDS PostgreSQL 14 (db.t3.micro)│
         │  wildfire_db / wildfire_events │
         └──────────────────────────────┘
```

**Why this is the minimal good choice**

| Choice | Rationale |
|--------|-----------|
| Single EC2 + Docker | No ECS/Kubernetes ops; matches how you already run the map service |
| RDS PostgreSQL | Managed backups, security groups, free-tier eligible |
| ALB | Stable HTTPS hostname, health checks; optional but professional |
| No Kafka on demo path | Cuts cost, ports, and failure modes; portfolio still documents full pipeline in README |
| Reuse `map-api` | Same Folium + SQL + OSM proxy as production; no second backend |

---

## What was added in the repo

| Item | Purpose |
|------|---------|
| `GET /` | Static dashboard (map + table + filters) |
| `GET /demo/wildfires` | JSON API for dashboard |
| `GET /demo/stats` | Aggregate counts |
| `POST /demo/sync` | FIRMS → Postgres refresh (API key) |
| `docker-compose.demo.yml` | Postgres + map-api only |
| `postgres/seed/demo_sample.sql` | Sample data without FIRMS |
| `scripts/sync_firms_to_postgres.py` | CLI sync without Kafka |
| `env.demo.example` | Demo environment template |

---

## Local demo (before AWS)

```bash
cp env.demo.example .env
# Set POSTGRES_PASSWORD (and FIRMS_MAP_KEY if you will sync live data)

docker compose -f docker-compose.demo.yml up -d --build
chmod +x scripts/seed_demo_data.sh
./scripts/seed_demo_data.sh

open http://localhost:8003/        # dashboard
open http://localhost:8003/docs    # Swagger
open http://localhost:8003/map     # Folium map
```

Refresh from NASA FIRMS (requires real `FIRMS_MAP_KEY`, `AUTH_ENABLED=true`, `WILDFIRE_API_KEY`):

```bash
curl -X POST "http://localhost:8003/demo/sync" \
  -H "X-API-Key: $WILDFIRE_API_KEY"
```

---

## AWS deployment checklist

### Phase 1 — Database (RDS)

1. **Create RDS PostgreSQL 14** (Single-AZ, `db.t3.micro`, 20 GB gp2).
2. **VPC**: default VPC is fine for a portfolio demo.
3. **Credentials**: master user + strong password; note endpoint hostname.
4. **Security group `rds-demo-sg`**: inbound TCP **5432** only from the app security group (see below).
5. **Initialize schema** (from a machine that can reach RDS, or EC2 bastion):

   ```bash
   psql "host=<rds-endpoint> port=5432 dbname=postgres user=<user> sslmode=require" \
     -f postgres/init/init.sql
   ```

   Then connect to `wildfire_db` and seed:

   ```bash
   psql "host=<rds-endpoint> dbname=wildfire_db user=<user> sslmode=require" \
     -f postgres/seed/demo_sample.sql
   ```

6. *(Optional)* Run `scripts/sync_firms_to_postgres.py` from EC2 with `POSTGRES_HOST=<rds-endpoint>` and `FIRMS_MAP_KEY`.

### Phase 2 — Application EC2

1. **Launch EC2** Amazon Linux 2023 or Ubuntu 22.04, `t3.micro` (free tier).
2. **Security group `app-demo-sg`**:
   - Inbound **80** and **443** from `0.0.0.0/0` (or restrict to your IP while testing).
   - Inbound **22** from your IP only (SSH).
   - Outbound: all (for OSM tiles + ECR/docker pulls).
3. **User data / SSH setup**: install Docker + Docker Compose plugin.
4. **Clone repo** on the instance (or copy `docker-compose.demo.yml`, `services/map`, `airflow/etl`, `postgres/`).
5. **Create `/app/.env`** on the server:

   ```env
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=<rds-password>
   POSTGRES_HOST=<rds-endpoint>
   POSTGRES_PORT=5432
   POSTGRES_DB=wildfire_db
   FIRMS_MAP_KEY=<key-or-placeholder>
   DEMO_MODE=true
   MAP_CONSUMER_ENABLED=false
   MAP_PUBLIC_BASE_URL=https://your-domain.example.com
   AUTH_ENABLED=false
   OSM_TILE_USE_PROXY=true
   ```

6. **Adjust `docker-compose.demo.yml` for RDS**: set `POSTGRES_HOST` to the RDS endpoint (not `postgres` service). Two options:
   - **A)** Run only the `map-api` container on EC2 and point env at RDS (remove local `postgres` service).
   - **B)** Keep compose Postgres for dev; on AWS use an override file `docker-compose.demo.aws.yml` with external RDS env vars.

   Example override (map-api only):

   ```yaml
   services:
     map-api:
       environment:
         POSTGRES_HOST: ${POSTGRES_HOST}
       depends_on: []
   ```

7. **Build and run**:

   ```bash
   docker compose -f docker-compose.demo.yml up -d --build map-api
   ```

### Phase 3 — Public URL

**Option A — Application Load Balancer (recommended)**

1. Create **target group** (HTTP, port **8003**, health check `GET /health`).
2. Register EC2 instance.
3. Create **ALB** (internet-facing) listener **80** → target group.
4. *(Optional)* ACM certificate + HTTPS listener **443**.
5. Set `MAP_PUBLIC_BASE_URL` to `http(s)://<alb-dns-name>` and restart map-api.

**Option B — Direct EC2 (cheapest, less polished)**

1. Map host port `8003:8003` (already in demo compose).
2. Open security group port **8003** (not ideal for production; use ALB instead).
3. Public URL: `http://<ec2-public-ip>:8003/`

### Phase 4 — DNS (optional)

1. Route 53 alias A record → ALB.
2. Update `MAP_PUBLIC_BASE_URL` to your domain.

### Phase 5 — Verify

| Check | URL / command |
|-------|----------------|
| Health | `curl https://<host>/health` |
| Dashboard | Browser `https://<host>/` |
| API | `https://<host>/demo/wildfires?limit=10` |
| Swagger | `https://<host>/docs` |
| Map | `https://<host>/map` |

---

## Environment variables (demo)

| Variable | Required | Notes |
|----------|----------|-------|
| `POSTGRES_PASSWORD` | Yes | RDS / local DB |
| `POSTGRES_HOST` | Yes | `postgres` in compose; RDS endpoint on AWS |
| `FIRMS_MAP_KEY` | Yes* | Config init requires it; placeholder OK if read-only |
| `MAP_PUBLIC_BASE_URL` | Recommended | OSM tile proxy Referer |
| `DEMO_MODE` | Recommended | `true` on public hosts |
| `MAP_CONSUMER_ENABLED` | Recommended | `false` |
| `AUTH_ENABLED` | Optional | `false` for public read demo |
| `WILDFIRE_API_KEY` | If auth on | Protects `/demo/sync`, `/generate` |

---

## Cost estimate (free tier friendly)

| Resource | Approx. monthly |
|----------|-----------------|
| EC2 t3.micro | Free tier 12 months |
| RDS db.t3.micro | Free tier 12 months |
| ALB | ~$16+ if left running 24/7 — skip for lowest cost (use EC2 IP) |
| Data transfer | Low for demo traffic |

---

## Risks and simplifications

| Simplification | Impact |
|----------------|--------|
| No Kafka on demo path | Live ingest is batch/sync only; no real-time streaming demo |
| `AUTH_ENABLED=false` default | Public read endpoints; enable auth before `/demo/sync` in production |
| Single EC2 | No auto-scaling; fine for portfolio traffic |
| Shared map-api image still lists `confluent-kafka` | Dependency present but unused when consumer disabled |
| Sample seed data | Small static set; run FIRMS sync for realistic volume |
| No WAF / rate limiting | Add CloudFront or API Gateway if abused |

---

## Code change summary

**Modified**

- `services/map/app/main.py` — demo routes, static dashboard, lazy Kafka consumer import
- `services/map/Dockerfile` — copy `static/`
- `services/map/requirements.txt` — `requests` for FIRMS sync

**New**

- `airflow/etl/demo_queries.py`, `postgres_write.py`, `demo_sync.py`
- `services/map/static/` — `index.html`, `styles.css`, `dashboard.js`
- `docker-compose.demo.yml`, `env.demo.example`
- `postgres/seed/demo_sample.sql`
- `scripts/sync_firms_to_postgres.py`, `scripts/seed_demo_data.sh`
- `docs/PUBLIC_DEMO_DEPLOYMENT.md` (this file)

**Unchanged (production path)**

- Full `docker-compose.yml`, Kafka topics, Airflow DAG, all workers

---

## Maintenance

| Task | How |
|------|-----|
| Refresh data | `POST /demo/sync` or `scripts/sync_firms_to_postgres.py` |
| Regenerate Folium file | `POST /generate` (auth) or restart service |
| Show full pipeline | Run main compose locally; document in README |

For questions about the production event-driven design, see `README.md` and `docs/CODEBASE_WALKTHROUGH.md`.
