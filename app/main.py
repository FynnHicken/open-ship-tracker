import os
import json
import asyncio
import re
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("open-ship-tracker")

API_KEY = os.environ.get("DATADOCKED_API_KEY", "")
AISSTREAM_KEY = os.environ.get("AISSTREAM_API_KEY", "")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
VESSELS_FILE = os.path.join(DATA_DIR, "vessels.json")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
DB_FILE = os.path.join(DATA_DIR, "archive.db")
SEED_FILE = os.path.join(os.path.dirname(__file__), "vessels_seed.json")

app = FastAPI(
    title="OpenShipTracker",
    description="Open Source AIS Vessel Tracker & Voyage History with Live WebSockets and Route Filtering",
    version="1.0.0"
)

NAV_STATUS_MAP = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuvrability",
    4: "Constrained by her draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    15: "Active (AIS)"
}

refresh_state = {
    "is_refreshing": False,
    "current": 0,
    "total": 0,
    "last_run": None,
    "errors": [],
    "aisstream_connected": False,
    "aisstream_messages": 0
}

class VesselAddRequest(BaseModel):
    mmsi: str
    name: str
    callsign: Optional[str] = ""
    imo: Optional[str] = ""
    category: Optional[str] = "Forschung"
    class_name: Optional[str] = ""
    homeport: Optional[str] = ""

class SettingsRequest(BaseModel):
    auto_refresh_daily: bool
    refresh_interval_minutes: Optional[int] = 10
    api_key: Optional[str] = None
    aisstream_key: Optional[str] = None

def safe_float(val, default=0.0):
    if val is None or val == "" or val == "-":
        return default
    try:
        return float(val)
    except:
        return default

# --- SQLITE ARCHIVE DATABASE ---
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mmsi TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                speed REAL,
                course REAL,
                heading REAL,
                status TEXT,
                destination TEXT,
                timestamp TEXT NOT NULL,
                source TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mmsi_time ON position_history(mmsi, timestamp)")

def record_position_to_archive(mmsi: str, lat, lon, speed=None, course=None, heading=None, status=None, destination=None, timestamp=None, source="AIS"):
    f_lat = safe_float(lat, None)
    f_lon = safe_float(lon, None)
    if f_lat is None or f_lon is None:
        return

    f_speed = safe_float(speed, None)
    f_course = safe_float(course, None)
    f_heading = safe_float(heading, None)

    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT latitude, longitude, timestamp FROM position_history WHERE mmsi = ? ORDER BY id DESC LIMIT 1", (mmsi,))
            row = cur.fetchone()
            if row:
                last_lat, last_lon, last_time = row
                if abs(last_lat - f_lat) < 0.0001 and abs(last_lon - f_lon) < 0.0001:
                    return
            conn.execute("""
                INSERT INTO position_history (mmsi, latitude, longitude, speed, course, heading, status, destination, timestamp, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (mmsi, f_lat, f_lon, f_speed, f_course, f_heading, status, destination, timestamp, source))
    except Exception as e:
        logger.error(f"Error saving to archive db: {e}")

def get_track_history(
    mmsi: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 5000
) -> List[Dict[str, Any]]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            query = """
                SELECT latitude, longitude, speed, course, heading, status, timestamp, source
                FROM position_history
                WHERE mmsi = ?
            """
            params: List[Any] = [mmsi]
            
            if start_time:
                clean_start = start_time.strip().replace('T', ' ')
                if len(clean_start) == 16:
                    clean_start += ':00'
                query += " AND replace(replace(timestamp, 'T', ' '), 'Z', '') >= ?"
                params.append(clean_start)
                
            if end_time:
                clean_end = end_time.strip().replace('T', ' ')
                if len(clean_end) == 16:
                    clean_end += ':59'
                query += " AND replace(replace(timestamp, 'T', ' '), 'Z', '') <= ?"
                params.append(clean_end)
                
            query += " ORDER BY replace(replace(timestamp, 'T', ' '), 'Z', '') ASC LIMIT ?"
            params.append(limit)
            
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Error fetching track history: {e}")
        return []

def get_track_range(mmsi: str) -> Dict[str, Any]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT min(timestamp), max(timestamp), count(*) 
                FROM position_history 
                WHERE mmsi = ?
            """, (mmsi,))
            row = cur.fetchone()
            if row and row[2] > 0:
                return {
                    "mmsi": mmsi,
                    "min_time": row[0],
                    "max_time": row[1],
                    "total_points": row[2]
                }
    except Exception as e:
        logger.error(f"Error fetching track range: {e}")
    return {"mmsi": mmsi, "min_time": None, "max_time": None, "total_points": 0}

def load_vessels() -> List[Dict[str, Any]]:
    seed_data = []
    if os.path.exists(SEED_FILE):
        try:
            with open(SEED_FILE, "r", encoding="utf-8") as f:
                seed_data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading seed file: {e}")

    if not os.path.exists(VESSELS_FILE):
        if seed_data:
            save_vessels(seed_data)
        return seed_data

    try:
        with open(VESSELS_FILE, "r", encoding="utf-8") as f:
            vessels = json.load(f)
        
        existing_mmsi = {str(v.get("mmsi")) for v in vessels}
        changed = False
        for s in seed_data:
            if str(s.get("mmsi")) not in existing_mmsi:
                vessels.append(s)
                changed = True
        if changed:
            save_vessels(vessels)
        return vessels
    except Exception as e:
        logger.error(f"Error reading vessels file: {e}")
        return []

def save_vessels(data: List[Dict[str, Any]]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(VESSELS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_config() -> Dict[str, Any]:
    default_cfg = {
        "auto_refresh_daily": False,
        "refresh_interval_minutes": 10,
        "api_key": API_KEY,
        "aisstream_key": AISSTREAM_KEY
    }
    if not os.path.exists(CONFIG_FILE):
        save_config(default_cfg)
        return default_cfg
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            changed = False
            for k, v in default_cfg.items():
                if k not in cfg:
                    cfg[k] = v
                    changed = True
            if changed:
                save_config(cfg)
            return cfg
    except:
        return default_cfg

def save_config(cfg: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

# --- AISSTREAM REAL-TIME LISTENER ---
async def aisstream_listener():
    """Background task connecting to AISStream.io WebSocket for real-time AIS reports."""
    while True:
        cfg = load_config()
        key = cfg.get("aisstream_key") or AISSTREAM_KEY
        if not key:
            refresh_state["aisstream_connected"] = False
            await asyncio.sleep(10)
            continue

        vessels = load_vessels()
        mmsi_list = [str(v["mmsi"]).strip() for v in vessels if v.get("mmsi")]

        if not mmsi_list:
            await asyncio.sleep(5)
            continue

        url = "wss://stream.aisstream.io/v0/stream"
        logger.info(f"Connecting to AISStream.io for {len(mmsi_list)} vessels...")
        
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                refresh_state["aisstream_connected"] = True
                sub = {
                    "APIKey": key,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],
                    "FiltersShipMMSI": mmsi_list[:250],
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                await ws.send(json.dumps(sub))
                logger.info("AISStream subscription active. Listening for real-time messages...")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        m_type = msg.get("MessageType")
                        meta = msg.get("MetaData", {})
                        mmsi = str(meta.get("MMSI") or "")
                        ship_name = (meta.get("ShipName") or "").strip()

                        if not mmsi:
                            continue

                        refresh_state["aisstream_messages"] += 1

                        current_vessels = load_vessels()
                        target = None
                        for v in current_vessels:
                            if str(v.get("mmsi")) == mmsi:
                                target = v
                                break

                        if not target:
                            continue

                        now_iso = datetime.now(timezone.utc).isoformat()
                        if not target.get("last_position"):
                            target["last_position"] = {}

                        pos = target["last_position"]

                        if ship_name:
                            target["current_ais_name"] = ship_name
                            pos["name"] = ship_name

                        if m_type == "PositionReport":
                            p = msg.get("Message", {}).get("PositionReport", {})
                            lat = p.get("Latitude")
                            lon = p.get("Longitude")
                            sog = p.get("Sog")
                            cog = p.get("Cog")
                            hdg = p.get("TrueHeading")
                            nav_stat = p.get("NavigationalStatus")
                            stat_text = NAV_STATUS_MAP.get(nav_stat, "Active (AIS)")

                            if lat is not None and lon is not None:
                                pos["latitude"] = str(lat)
                                pos["longitude"] = str(lon)
                                pos["speed"] = str(sog) if sog is not None else "0.0"
                                pos["course"] = str(cog) if cog is not None else "0"
                                pos["heading"] = str(hdg) if hdg is not None and hdg < 511 else str(cog or 0)
                                pos["navigationalStatus"] = stat_text
                                pos["dataSource"] = "AISStream Live"
                                pos["positionReceived"] = meta.get("time_utc") or now_iso
                                pos["fetched_at"] = now_iso
                                save_vessels(current_vessels)

                                record_position_to_archive(
                                    mmsi=mmsi,
                                    lat=float(lat),
                                    lon=float(lon),
                                    speed=float(sog) if sog is not None else None,
                                    course=float(cog) if cog is not None else None,
                                    heading=float(hdg) if hdg is not None else None,
                                    status=stat_text,
                                    destination=pos.get("destination"),
                                    timestamp=meta.get("time_utc") or now_iso,
                                    source="AISStream Live"
                                )
                                logger.info(f"Live AIS: {target.get('name')} ({mmsi}) @ {lat}, {lon}")

                        elif m_type == "ShipStaticData":
                            s = msg.get("Message", {}).get("ShipStaticData", {})
                            dest = s.get("Destination")
                            call = s.get("CallSign")
                            if dest:
                                pos["destination"] = dest.strip()
                            if call:
                                target["callsign"] = call.strip()
                            save_vessels(current_vessels)

                    except Exception as parse_err:
                        logger.warning(f"Error parsing AISStream message: {parse_err}")

        except Exception as conn_err:
            refresh_state["aisstream_connected"] = False
            logger.warning(f"AISStream disconnected ({conn_err}). Retrying in 5s...")
            await asyncio.sleep(5)

# --- REST API LOOKUP (OPTIONAL DATADOCKED / VESSELFINDER INTEGRATION) ---
async def fetch_vessel_location(mmsi_or_imo: str, api_key: str) -> Optional[Dict[str, Any]]:
    if not api_key:
        return None
    url = f"https://datadocked.com/api/vessels_operations/get-vessel-location?imo_or_mmsi={mmsi_or_imo}"
    headers = {"x-api-key": api_key}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(url, headers=headers)
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, dict) and "latitude" in data and data.get("latitude"):
                    data["fetched_at"] = datetime.now(timezone.utc).isoformat()
                    return data
    except Exception as e:
        logger.error(f"Error fetching location for {mmsi_or_imo}: {e}")
    return None

async def run_refresh_all_task():
    global refresh_state
    if refresh_state["is_refreshing"]:
        return
    cfg = load_config()
    current_key = cfg.get("api_key") or API_KEY
    if not current_key:
        logger.info("No REST API key configured for bulk refresh.")
        return
        
    vessels = load_vessels()
    refresh_state["is_refreshing"] = True
    refresh_state["total"] = len(vessels)
    refresh_state["current"] = 0
    refresh_state["errors"] = []
    try:
        for idx, vessel in enumerate(vessels):
            refresh_state["current"] = idx + 1
            mmsi = vessel.get("mmsi")
            try:
                pos = await fetch_vessel_location(mmsi, current_key)
                if pos:
                    vessel["last_position"] = pos
                    record_position_to_archive(
                        mmsi=mmsi,
                        lat=float(pos["latitude"]),
                        lon=float(pos["longitude"]),
                        speed=float(pos.get("speed") or 0),
                        course=float(pos.get("course") or 0),
                        heading=float(pos.get("heading") or 0),
                        status=pos.get("navigationalStatus"),
                        destination=pos.get("destination"),
                        timestamp=pos.get("positionReceived") or pos.get("fetched_at"),
                        source="REST API"
                    )
                else:
                    refresh_state["errors"].append(f"{vessel.get('name', mmsi)}: No recent position")
            except Exception as e:
                refresh_state["errors"].append(f"{vessel.get('name', mmsi)}: {str(e)}")
            await asyncio.sleep(0.5)
        save_vessels(vessels)
        refresh_state["last_run"] = datetime.now(timezone.utc).isoformat()
    finally:
        refresh_state["is_refreshing"] = False

# --- API ENDPOINTS ---
@app.get("/api/vessels")
async def get_vessels():
    vessels = load_vessels()
    with_pos = sum(1 for v in vessels if v.get("last_position") and v["last_position"].get("latitude"))
    return {
        "vessels": vessels,
        "total": len(vessels),
        "with_position": with_pos,
        "refresh_state": refresh_state
    }

@app.get("/api/history/{mmsi}")
async def get_history(
    mmsi: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 5000
):
    """Returns stored track coordinates and history for a vessel, filtered by start and end datetime."""
    points = get_track_history(mmsi, start_time=start, end_time=end, limit=limit)
    return {
        "mmsi": mmsi,
        "total_points": len(points),
        "start": start,
        "end": end,
        "points": points
    }

@app.get("/api/history/{mmsi}/range")
async def get_history_range_endpoint(mmsi: str):
    """Returns available timestamp range and total point count for a vessel."""
    return get_track_range(mmsi)

@app.post("/api/vessels")
async def add_vessel(req: VesselAddRequest):
    vessels = load_vessels()
    for v in vessels:
        if str(v.get("mmsi")) == str(req.mmsi):
            raise HTTPException(status_code=400, detail="Vessel with this MMSI already exists")
    new_v = {
        "mmsi": req.mmsi.strip(),
        "name": req.name.strip(),
        "callsign": req.callsign.strip() if req.callsign else "",
        "imo": req.imo.strip() if req.imo else "",
        "category": req.category.strip() if req.category else "Sonstige",
        "class_name": req.class_name.strip() if req.class_name else "",
        "homeport": req.homeport.strip() if req.homeport else "",
        "current_ais_name": None,
        "last_position": None
    }
    vessels.append(new_v)
    save_vessels(vessels)
    return {"status": "ok", "vessel": new_v}

@app.delete("/api/vessels/{mmsi}")
async def delete_vessel(mmsi: str):
    vessels = load_vessels()
    updated = [v for v in vessels if str(v.get("mmsi")) != str(mmsi)]
    if len(updated) == len(vessels):
        raise HTTPException(status_code=404, detail="Vessel not found")
    save_vessels(updated)
    return {"status": "ok", "deleted": mmsi}

@app.post("/api/refresh/{mmsi}")
async def refresh_single(mmsi: str):
    cfg = load_config()
    current_key = cfg.get("api_key") or API_KEY
    if not current_key:
        return {"status": "error", "message": "No REST API key configured. Use AISStream or set key in Settings."}
    vessels = load_vessels()
    target = None
    for v in vessels:
        if str(v.get("mmsi")) == str(mmsi):
            target = v
            break
    if not target:
        raise HTTPException(status_code=404, detail="Vessel not in fleet")
    pos = await fetch_vessel_location(mmsi, current_key)
    if not pos:
        return {"status": "ok", "updated": False, "message": "No recent AIS position received", "vessel": target}
    target["last_position"] = pos
    record_position_to_archive(
        mmsi=mmsi,
        lat=float(pos["latitude"]),
        lon=float(pos["longitude"]),
        speed=float(pos.get("speed") or 0),
        course=float(pos.get("course") or 0),
        heading=float(pos.get("heading") or 0),
        status=pos.get("navigationalStatus"),
        destination=pos.get("destination"),
        timestamp=pos.get("positionReceived") or pos.get("fetched_at"),
        source="REST API"
    )
    save_vessels(vessels)
    return {"status": "ok", "updated": True, "vessel": target}

@app.post("/api/refresh-all")
async def trigger_refresh_all(background_tasks: BackgroundTasks):
    if refresh_state["is_refreshing"]:
        return JSONResponse(status_code=409, content={"status": "already_running", "state": refresh_state})
    background_tasks.add_task(run_refresh_all_task)
    return {"status": "started", "total": len(load_vessels())}

@app.get("/api/status")
async def get_status():
    cfg = load_config()
    # Mask API keys in public status output
    safe_cfg = {
        "auto_refresh_daily": cfg.get("auto_refresh_daily", False),
        "refresh_interval_minutes": cfg.get("refresh_interval_minutes", 10),
        "has_api_key": bool(cfg.get("api_key")),
        "has_aisstream_key": bool(cfg.get("aisstream_key"))
    }
    return {"refresh_state": refresh_state, "config": safe_cfg}

@app.post("/api/settings")
async def update_settings(req: SettingsRequest):
    cfg = load_config()
    cfg["auto_refresh_daily"] = req.auto_refresh_daily
    if req.refresh_interval_minutes is not None:
        cfg["refresh_interval_minutes"] = max(1, min(120, req.refresh_interval_minutes))
    if req.api_key is not None:
        cfg["api_key"] = req.api_key.strip()
    if req.aisstream_key is not None:
        cfg["aisstream_key"] = req.aisstream_key.strip()
    save_config(cfg)
    return {"status": "ok", "message": "Settings updated"}

@app.on_event("startup")
async def startup_event():
    init_db()
    
    # Preload positions from seed/vessels.json into archive
    vessels = load_vessels()
    for v in vessels:
        pos = v.get("last_position")
        if pos and pos.get("latitude") and pos.get("longitude"):
            record_position_to_archive(
                mmsi=v.get("mmsi"),
                lat=pos.get("latitude"),
                lon=pos.get("longitude"),
                speed=pos.get("speed"),
                course=pos.get("course"),
                heading=pos.get("heading"),
                status=pos.get("navigationalStatus"),
                destination=pos.get("destination"),
                timestamp=pos.get("positionReceived") or pos.get("fetched_at"),
                source=pos.get("dataSource") or "Archive"
            )

    asyncio.create_task(aisstream_listener())

    async def daily_worker():
        while True:
            await asyncio.sleep(60)
            now = datetime.now()
            cfg = load_config()
            if cfg.get("auto_refresh_daily"):
                last_run = refresh_state.get("last_run")
                today_str = now.strftime("%Y-%m-%d")
                if now.hour == 6 and now.minute == 0 and (not last_run or not last_run.startswith(today_str)):
                    logger.info("Triggering automatic daily refresh")
                    await run_refresh_all_task()
    asyncio.create_task(daily_worker())

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def root_index():
    return FileResponse(os.path.join(static_dir, "index.html"))
