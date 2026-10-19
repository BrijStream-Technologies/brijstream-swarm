#!/usr/bin/env python3
"""
BrijStream Swarm OS — Cloud API & Mission Control Service
=========================================================
FastAPI application serving the real-time Mission Control dashboard,
exposing remote activation/pause controls, and streaming continental telemetry.
"""

import os
import json
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pan_african_harvester import swarm, DATABASE_URL, POSTGRES_AVAILABLE

BASE_DIR = Path(__file__).resolve().parent
STATIC_INDEX = BASE_DIR / "static" / "index.html"
SWARM_API_KEY = os.getenv("SWARM_API_KEY", "brijstream_swarm_secret_key_2026")

app = FastAPI(
    title="BrijStream Swarm OS — Cloud Mission Control",
    description="Remote control plane and telemetry engine for Pan-African Creative Swarm on Railway",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class StartRequest(BaseModel):
    hours: Optional[float] = 24.0


def verify_api_key(authorization: Optional[str] = Header(None)):
    if not SWARM_API_KEY:
        return True
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header"
        )
    token = authorization.replace("Bearer ", "").strip()
    if token != SWARM_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Swarm API Key"
        )
    return True


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "brijstream-swarm-cloud",
        "database": "PostgreSQL" if (DATABASE_URL and POSTGRES_AVAILABLE) else "SQLite"
    }


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    if STATIC_INDEX.exists():
        with open(STATIC_INDEX, "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content=content)
    return HTMLResponse("<h1>BrijStream Swarm OS Active</h1><p>Dashboard file initializing...</p>")


@app.get("/api/swarm/status")
def get_swarm_status():
    stats = swarm.get_stats()
    return {
        "status": "success",
        "data": stats
    }


@app.post("/api/swarm/start")
def start_swarm(payload: StartRequest = StartRequest(), authorization: Optional[str] = Header(None)):
    verify_api_key(authorization)
    result = swarm.start(hours=payload.hours)
    return result


@app.post("/api/swarm/pause")
def pause_swarm(authorization: Optional[str] = Header(None)):
    verify_api_key(authorization)
    result = swarm.pause()
    return result


@app.get("/api/creatives/recent")
def get_recent_creatives(limit: int = Query(50, le=500)):
    conn = swarm.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
        SELECT entity_uid, name, stage_or_brand_name, entity_type, region, country,
               city_or_hub, subgenres_or_niches, instagram_handle, cmo_affiliation,
               follower_tier, estimated_reach, observed_pain_point, bespoke_pitch_text
        FROM creative_entities
        ORDER BY id DESC
        LIMIT %s;
        """ if (DATABASE_URL and POSTGRES_AVAILABLE) else """
        SELECT entity_uid, name, stage_or_brand_name, entity_type, region, country,
               city_or_hub, subgenres_or_niches, instagram_handle, cmo_affiliation,
               follower_tier, estimated_reach, observed_pain_point, bespoke_pitch_text
        FROM creative_entities
        ORDER BY id DESC
        LIMIT ?;
        """, (limit,))
        rows = cur.fetchall()
        keys = [
            "entity_uid", "name", "brand_name", "entity_type", "region", "country",
            "city_or_hub", "subgenres", "handle", "cmo", "follower_tier",
            "estimated_reach", "pain_point", "bespoke_pitch"
        ]
        results = [dict(zip(keys, r)) for r in rows]
        return {"status": "success", "count": len(results), "data": results}
    finally:
        conn.close()


@app.get("/api/creatives/export")
def export_manifest():
    conn = swarm.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
        SELECT entity_uid, name, stage_or_brand_name, entity_type, region, country,
               city_or_hub, subgenres_or_niches, instagram_handle, bespoke_pitch_text
        FROM creative_entities
        ORDER BY id ASC
        LIMIT 1000;
        """)
        rows = cur.fetchall()
        keys = ["entity_uid", "name", "brand_name", "entity_type", "region", "country", "city_or_hub", "subgenres", "handle", "pitch"]
        data = [dict(zip(keys, r)) for r in rows]
        return JSONResponse(
            content=data,
            headers={"Content-Disposition": "attachment; filename=brijstream_creatives_manifest.json"}
        )
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
