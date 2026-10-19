#!/usr/bin/env python3
"""
BrijStream Swarm OS — Cloud Harvester Daemon (Railway Edition)
============================================================
Cloud-native continental harvester supporting both PostgreSQL (via DATABASE_URL)
and SQLite (on Railway Persistent Volumes). Provides thread-safe start, pause,
and live telemetry tracking.
"""

import os
import sys
import time
import json
import threading
import sqlite3
import datetime
import urllib.request
import urllib.parse
from pathlib import Path

# Optional psycopg2 import for PostgreSQL support
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL")
SQLITE_DB_PATH = Path(os.getenv("SQLITE_PATH", str(DATA_DIR / "creatives.db")))
STATE_FILE = DATA_DIR / "harvester_state.json"
LOG_FILE = DATA_DIR / "harvester_daemon.log"

USER_AGENT = "BrijStreamContinentalScout/2.6 (contact@brijstream.com; https://brijstream.com)"
MB_BASE = "https://musicbrainz.org/ws/2"

TERRITORY_CONFIG = {
    "ZA": {"name": "South Africa", "region": "Southern Africa", "default_hub": "Johannesburg / Soweto", "cmo": "SAMRO / CAPASSO", "tags": ["amapiano", "afro house", "gqom", "kwaito", "hip hop", "jazz", "gospel"]},
    "NG": {"name": "Nigeria", "region": "Western Africa", "default_hub": "Lagos", "cmo": "COSON / MCSN", "tags": ["afrobeats", "street pop", "alté", "fuji", "highlife", "hip hop", "gospel"]},
    "KE": {"name": "Kenya", "region": "Eastern Africa", "default_hub": "Nairobi", "cmo": "MCSK / KECOBO", "tags": ["gengetone", "benga", "kapuka", "afropop", "hip hop", "gospel"]},
    "GH": {"name": "Ghana", "region": "Western Africa", "default_hub": "Accra", "cmo": "GHAMRO", "tags": ["highlife", "hiplife", "afrobeats", "drill", "gospel"]},
    "TZ": {"name": "Tanzania", "region": "Eastern Africa", "default_hub": "Dar es Salaam", "cmo": "COSOTA", "tags": ["bongo flava", "singeli", "taarab", "afropop"]},
    "ZW": {"name": "Zimbabwe", "region": "Southern Africa", "default_hub": "Harare", "cmo": "ZIMURA", "tags": ["zimdancehall", "sungura", "afropop", "chimurenga"]},
    "UG": {"name": "Uganda", "region": "Eastern Africa", "default_hub": "Kampala", "cmo": "UPRS", "tags": ["kidandali", "dancehall", "afrobeats", "kadongo kamu"]},
    "CD": {"name": "DR Congo", "region": "Central & Pan-African", "default_hub": "Kinshasa", "cmo": "SOCODA", "tags": ["rumba", "soukous", "ndombolo"]},
    "CI": {"name": "Côte d'Ivoire", "region": "Western Africa", "default_hub": "Abidjan", "cmo": "BURIDA", "tags": ["coupé-décalé", "zouglou", "afropop"]},
    "SN": {"name": "Senegal", "region": "Western Africa", "default_hub": "Dakar", "cmo": "SODAV", "tags": ["mbalax", "afrobeats", "hip hop"]},
    "CM": {"name": "Cameroon", "region": "Central & Pan-African", "default_hub": "Douala / Yaoundé", "cmo": "SOCAM", "tags": ["makossa", "bikutsi", "afropop"]},
    "ZM": {"name": "Zambia", "region": "Southern Africa", "default_hub": "Lusaka", "cmo": "ZAMCOPS", "tags": ["zed beats", "kalindula", "afropop"]},
    "RW": {"name": "Rwanda", "region": "Eastern Africa", "default_hub": "Kigali", "cmo": "RSAU", "tags": ["afropop", "r&b", "hip hop"]},
    "EG": {"name": "Egypt", "region": "North Africa", "default_hub": "Cairo", "cmo": "SACER", "tags": ["shaabi", "mahraganat", "arabic pop", "trap"]},
    "MA": {"name": "Morocco", "region": "North Africa", "default_hub": "Casablanca", "cmo": "BMDA", "tags": ["gnawa", "chaabi", "rap", "rai"]},
}


class CloudHarvester:
    def __init__(self):
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        self.session_ingested = 0
        self.session_start = None
        self.target_seconds = 0
        self.target_label = ""
        self.last_status_message = "Idle"
        self.current_country = "None"
        self.current_mode = "None"
        self.init_db()

    def log(self, msg):
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def get_connection(self):
        if DATABASE_URL and POSTGRES_AVAILABLE:
            return psycopg2.connect(DATABASE_URL)
        else:
            conn = sqlite3.connect(SQLITE_DB_PATH, timeout=60.0)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=60000;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            return conn

    def init_db(self):
        conn = self.get_connection()
        is_pg = bool(DATABASE_URL and POSTGRES_AVAILABLE)
        cur = conn.cursor()

        if is_pg:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS creative_entities (
                id SERIAL PRIMARY KEY,
                entity_uid VARCHAR(100) UNIQUE,
                name VARCHAR(255) NOT NULL,
                stage_or_brand_name VARCHAR(255),
                entity_type VARCHAR(100),
                region VARCHAR(100),
                country VARCHAR(100),
                city_or_hub VARCHAR(150),
                subgenres_or_niches TEXT,
                whatsapp_number VARCHAR(100),
                bio_link_url TEXT,
                instagram_handle VARCHAR(100),
                tiktok_handle VARCHAR(100),
                twitter_handle VARCHAR(100),
                contact_email VARCHAR(255),
                cmo_affiliation VARCHAR(150),
                follower_tier VARCHAR(50),
                estimated_reach INTEGER,
                observed_pain_point TEXT,
                latest_notable_work TEXT,
                enrichment_status VARCHAR(50) DEFAULT 'VETTED',
                source_channel VARCHAR(100) DEFAULT 'MUSICBRAINZ_DAEMON',
                bespoke_pitch_text TEXT,
                last_verified_at VARCHAR(100)
            );
            CREATE INDEX IF NOT EXISTS idx_region ON creative_entities(region);
            CREATE INDEX IF NOT EXISTS idx_type ON creative_entities(entity_type);
            CREATE INDEX IF NOT EXISTS idx_country ON creative_entities(country);
            """)
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS creative_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_uid TEXT UNIQUE,
                name TEXT NOT NULL,
                stage_or_brand_name TEXT,
                entity_type TEXT,
                region TEXT,
                country TEXT,
                city_or_hub TEXT,
                subgenres_or_niches TEXT,
                whatsapp_number TEXT,
                bio_link_url TEXT,
                instagram_handle TEXT,
                tiktok_handle TEXT,
                twitter_handle TEXT,
                contact_email TEXT,
                cmo_affiliation TEXT,
                follower_tier TEXT,
                estimated_reach INTEGER,
                observed_pain_point TEXT,
                latest_notable_work TEXT,
                enrichment_status TEXT DEFAULT 'VETTED',
                source_channel TEXT DEFAULT 'MUSICBRAINZ_DAEMON',
                bespoke_pitch_text TEXT,
                last_verified_at TEXT
            );
            """)
        conn.commit()
        conn.close()

    def get_stats(self):
        conn = self.get_connection()
        cur = conn.cursor()
        is_pg = bool(DATABASE_URL and POSTGRES_AVAILABLE)

        try:
            if is_pg:
                cur.execute("SELECT COUNT(*) FROM creative_entities;")
                total = cur.fetchone()[0] or 0
            else:
                cur.execute("SELECT MAX(id) FROM creative_entities;")
                total = cur.fetchone()[0] or 0

            cur.execute("SELECT region, COUNT(*) FROM creative_entities GROUP BY region;")
            regions = dict(cur.fetchall())

            cur.execute("SELECT entity_type, COUNT(*) FROM creative_entities GROUP BY entity_type;")
            types = dict(cur.fetchall())
        except Exception:
            total = 0
            regions = {}
            types = {}
        finally:
            conn.close()

        return {
            "total_entities": total,
            "session_ingested": self.session_ingested,
            "is_running": self._running,
            "current_country": self.current_country,
            "current_mode": self.current_mode,
            "last_status_message": self.last_status_message,
            "session_start": self.session_start.isoformat() if self.session_start else None,
            "target_label": self.target_label,
            "regions": regions,
            "types": types,
            "db_backend": "PostgreSQL" if is_pg else "SQLite WAL"
        }

    def load_state(self):
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "total_harvested": 0,
            "current_country_idx": 0,
            "current_tag_idx": 0,
            "current_offset": 0,
            "mode": "creators_by_person",
            "batches_completed": 0
        }

    def save_state(self, state):
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            self.log(f"Error saving state: {e}")

    def query_musicbrainz(self, endpoint, query_str, limit=50, offset=0):
        encoded_query = urllib.parse.quote(query_str)
        url = f"{MB_BASE}/{endpoint}/?query={encoded_query}&limit={limit}&offset={offset}&fmt=json"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

        for attempt in range(4):
            if not self._running:
                return None
            try:
                time.sleep(1.2)
                with urllib.request.urlopen(req, timeout=15) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as he:
                if he.code in (429, 503):
                    wait_time = (attempt + 1) * 3
                    self.log(f"MusicBrainz backoff ({he.code}) on query '{query_str}'. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.log(f"HTTP Error {he.code} on '{query_str}': {he}")
                    return None
            except Exception as e:
                self.log(f"Connection Exception on '{query_str}': {e}")
                time.sleep(2)
        return None

    def determine_entity_attributes(self, item, country_code, is_label=False):
        cfg = TERRITORY_CONFIG.get(country_code, {
            "name": "African Territory",
            "region": "Continental Africa",
            "default_hub": "Regional Hub",
            "cmo": "National Copyright Society"
        })
        name = item.get("name", "Unknown Artist")
        disambig = (item.get("disambiguation") or "").lower()
        tags = [t.get("name", "").lower() for t in item.get("tags", [])]
        tag_str = ", ".join(tags[:3]) if tags else "Afropop"

        if is_label:
            entity_type = "indie_label"
        elif any(p in disambig for p in ["producer", "beatmaker", "sound engineer", "mix", "arranger"]) or any("producer" in t for t in tags):
            entity_type = "music_producer"
        elif any(f in disambig for f in ["film", "director", "actor", "comedy", "satire"]) or any("film" in t for t in tags):
            entity_type = "film_house"
        elif any(pd in disambig for pd in ["podcast", "radio", "presenter", "host"]) or any("podcast" in t for t in tags):
            entity_type = "podcast"
        else:
            entity_type = "recording_artist"

        friction_map = {
            "ZA": "90-day DSP royalty lag and aggregator commission eating away streaming revenue",
            "NG": "High international wire fees and foreign currency conversion losses from international DSPs",
            "KE": "CMO royalty collection delays and high mobile money transaction fees",
            "GH": "Radio royalty fragmentation and delayed West African digital distributions",
            "TZ": "Loss of mechanical publishing rights on viral street hits due to informal contracts",
            "ZW": "Currency volatility and difficult international bank settlements for music revenue",
            "CD": "Zero official streaming presence leading to unchecked digital piracy across Central Africa",
            "EG": "Streaming payout delays and lack of local micro-payment options for street producers",
            "MA": "Lack of direct-to-creator digital dollar withdrawals for North African acts"
        }
        pain_point = friction_map.get(country_code, "Lack of direct-to-wallet micro-streaming monetization across Africa")
        score = item.get("score", 80)
        reach = int(score) * 250 + 2500
        tier = f"MICRO ({reach//1000}k)" if reach < 50000 else f"MID ({reach//1000}k)"

        clean_handle = "@" + "".join(e for e in name.lower() if e.isalnum() or e == "_")[:20]
        pitch = (
            f"{name}, African creators deserve transparent, instant digital wallet payouts instead of "
            f"waiting 90 days for overseas DSP aggregators. BrijStream provides 0MB PWA streaming, "
            f"guaranteeing exactly 54.7% Net Master Recording share, 15.3% US Statutory Publishing, "
            f"and $0.00 withdrawal fees. Onboard your catalog directly at https://brijstream.com/apply?ref={clean_handle.lstrip('@')}"
        )
        area_name = item.get("area", {}).get("name") or cfg["default_hub"]

        return {
            "name": name,
            "stage_name": name,
            "entity_type": entity_type,
            "region": cfg["region"],
            "country": cfg["name"],
            "city_or_hub": area_name,
            "subgenres": tag_str,
            "handle": clean_handle,
            "cmo": cfg["cmo"],
            "follower_tier": tier,
            "estimated_reach": reach,
            "observed_pain_point": pain_point,
            "latest_work": f"MusicBrainz Catalog ID: {item.get('id')}",
            "pitch_text": pitch,
            "mb_gid": item.get("id")
        }

    def _harvest_loop(self):
        conn = self.get_connection()
        cur = conn.cursor()
        is_pg = bool(DATABASE_URL and POSTGRES_AVAILABLE)

        stats = self.get_stats()
        initial_total = stats["total_entities"]

        state = self.load_state()
        country_codes = list(TERRITORY_CONFIG.keys())
        MODES = ["creators_by_person", "creators_by_genre", "producers", "indie_labels"]
        mode_idx = 0

        self.log(f"Swarm Harvester started in cloud worker. Target: {self.target_label}. Baseline: {initial_total:,}")

        while self._running:
            elapsed = (datetime.datetime.now(datetime.timezone.utc) - self.session_start).total_seconds()
            if self.target_seconds and elapsed >= self.target_seconds:
                self.log(f"Reached target duration of {self.target_seconds/3600:.2f} hours. Pausing swarm.")
                break

            c_idx = state["current_country_idx"] % len(country_codes)
            c_code = country_codes[c_idx]
            cfg = TERRITORY_CONFIG[c_code]
            self.current_country = f"{cfg['name']} ({c_code})"

            curr_mode = MODES[mode_idx % len(MODES)]
            self.current_mode = curr_mode
            curr_offset = state["current_offset"]

            if curr_mode == "creators_by_person":
                query = f"country:{c_code} AND (type:Person OR type:Group)"
                endpoint = "artist"
                is_label = False
                label_text = f"Artists & Groups: Country={c_code} ({cfg['name']})"
            elif curr_mode == "creators_by_genre":
                tags = cfg["tags"]
                curr_tag = tags[state["current_tag_idx"] % len(tags)]
                query = f"country:{c_code} AND tag:{curr_tag}"
                endpoint = "artist"
                is_label = False
                label_text = f"Genre Creators: Country={c_code} | Tag={curr_tag}"
            elif curr_mode == "producers":
                query = f"country:{c_code} AND (tag:producer OR tag:beatmaker OR tag:sound engineer)"
                endpoint = "artist"
                is_label = False
                label_text = f"Producers & Beatmakers: Country={c_code}"
            else:
                query = f"country:{c_code}"
                endpoint = "label"
                is_label = True
                label_text = f"Indie Labels: Country={c_code}"

            self.last_status_message = f"[{label_text}] Offset={curr_offset}"
            self.log(self.last_status_message)
            res = self.query_musicbrainz(endpoint, query, limit=50, offset=curr_offset)

            if res and self._running:
                items = res.get("artists" if not is_label else "labels", [])
                total_count = res.get("count", 0)

                if items:
                    batch_inserted = 0
                    for item in items:
                        if not self._running:
                            break
                        data = self.determine_entity_attributes(item, c_code, is_label=is_label)
                        uid = f"{c_code}_{data['entity_type'][:4].upper()}_{initial_total + self.session_ingested + batch_inserted + 1:07d}"

                        if is_pg:
                            cur.execute("""
                            INSERT INTO creative_entities (
                                entity_uid, name, stage_or_brand_name, entity_type, region, country, city_or_hub,
                                subgenres_or_niches, instagram_handle, cmo_affiliation, follower_tier,
                                estimated_reach, observed_pain_point, latest_notable_work, enrichment_status,
                                source_channel, bespoke_pitch_text, last_verified_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VETTED', 'MUSICBRAINZ_DAEMON', %s, %s)
                            ON CONFLICT (entity_uid) DO NOTHING;
                            """, (
                                uid, data["name"], data["stage_name"], data["entity_type"], data["region"],
                                data["country"], data["city_or_hub"], data["subgenres"], data["handle"],
                                data["cmo"], data["follower_tier"], data["estimated_reach"],
                                data["observed_pain_point"], data["latest_work"], data["pitch_text"],
                                datetime.datetime.now(datetime.timezone.utc).isoformat()
                            ))
                        else:
                            cur.execute("""
                            INSERT OR IGNORE INTO creative_entities (
                                entity_uid, name, stage_or_brand_name, entity_type, region, country, city_or_hub,
                                subgenres_or_niches, instagram_handle, cmo_affiliation, follower_tier,
                                estimated_reach, observed_pain_point, latest_notable_work, enrichment_status,
                                source_channel, bespoke_pitch_text, last_verified_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'VETTED', 'MUSICBRAINZ_DAEMON', ?, ?)
                            """, (
                                uid, data["name"], data["stage_name"], data["entity_type"], data["region"],
                                data["country"], data["city_or_hub"], data["subgenres"], data["handle"],
                                data["cmo"], data["follower_tier"], data["estimated_reach"],
                                data["observed_pain_point"], data["latest_work"], data["pitch_text"],
                                datetime.datetime.now(datetime.timezone.utc).isoformat()
                            ))
                        if cur.rowcount > 0:
                            batch_inserted += 1

                    conn.commit()
                    self.session_ingested += batch_inserted
                    state["total_harvested"] += batch_inserted
                    self.log(f"  ✓ Ingested {batch_inserted} new entities. Session: +{self.session_ingested:,} (DB Total: {initial_total + self.session_ingested:,})")

                    state["current_offset"] += 50
                    if state["current_offset"] >= min(total_count, 350):
                        state["current_offset"] = 0
                        mode_idx += 1
                        if mode_idx % len(MODES) == 0:
                            state["current_country_idx"] += 1
                            state["current_tag_idx"] += 1
                else:
                    state["current_offset"] = 0
                    mode_idx += 1
                    if mode_idx % len(MODES) == 0:
                        state["current_country_idx"] += 1
                        state["current_tag_idx"] += 1
            else:
                state["current_offset"] = 0
                mode_idx += 1
                if mode_idx % len(MODES) == 0:
                    state["current_country_idx"] += 1

            state["batches_completed"] += 1
            self.save_state(state)
            time.sleep(1.0)

        conn.commit()
        conn.close()
        self._running = False
        self.last_status_message = "Paused"
        self.log("Swarm Harvester thread finished safely.")

    def start(self, hours=24.0):
        with self._lock:
            if self._running:
                return {"success": False, "message": "Swarm is already running", "stats": self.get_stats()}
            self._running = True
            self.session_start = datetime.datetime.now(datetime.timezone.utc)
            self.target_seconds = hours * 3600
            target_dt = self.session_start + datetime.timedelta(hours=hours)
            self.target_label = f"{hours:.1f}h window (until {target_dt.strftime('%Y-%m-%d %H:%M:%S UTC')})"
            self._thread = threading.Thread(target=self._harvest_loop, daemon=True)
            self._thread.start()
            return {"success": True, "message": f"Swarm activated for {hours:.1f} hours", "stats": self.get_stats()}

    def pause(self):
        with self._lock:
            if not self._running:
                return {"success": False, "message": "Swarm is already paused", "stats": self.get_stats()}
            self._running = False
            self.last_status_message = "Pausing requested..."
            return {"success": True, "message": "Swarm pause signal sent safely", "stats": self.get_stats()}


# Global singleton instance for FastAPI
swarm = CloudHarvester()
