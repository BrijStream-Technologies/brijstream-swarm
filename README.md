# BrijStream Swarm OS — Cloud Engine

Autonomous Pan-African Creative Discovery & Campaign Swarm running on Railway.

## 🚀 Capabilities

1. **Continuous 24/7 Cloud Ingestion**:
   - Harvester daemon targeting creators, beatmakers, indie labels, and podcasters across 15 continental territories.
   - Dual-engine storage: High-speed **Railway PostgreSQL** (`DATABASE_URL`) with fallback to persistent volume SQLite.
2. **Interactive Mission Control Dashboard**:
   - Embedded at root (`/`) with real-time continental telemetry and 1-click WhatsApp deep-links.
3. **Agentic Remote Control**:
   - `GET /api/swarm/status` — Live worker telemetry, active territory, total entities.
   - `POST /api/swarm/start` — Activate harvester for N hours (`{"hours": 24}`).
   - `POST /api/swarm/pause` — Gracefully pause harvesting, flush commits, and checkpoint state.
   - `GET /api/creatives/recent` — Fetch latest ingested talent.
   - `GET /api/creatives/export` — Download staged outreach manifest.

## ⚙️ Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (auto-injected by Railway Postgres plugin).
- `SWARM_API_KEY`: Secret Bearer token for authorized start/pause controls.
- `PORT`: HTTP port (default: 8080).

## 🛡️ Brand Governance Invariants

- **Streaming Super-App Only**: Music, Podcasts, Series, Films, Micro-Dramas (no stem marketplace claims).
- **Exact Statutory Splits**: 54.7% Net Master / 15.3% US Statutory Mechanicals / 30.0% Platform Retention / $0.00 withdrawal fees.
