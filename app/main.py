import os
import json
import asyncio
import re
import sqlite3
import logging
import math
import time
import shutil
from datetime import datetime, timezone, timedelta
import hashlib
import secrets
from typing import Optional, List, Dict, Any, Tuple
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("open-ship-tracker")

AISSTREAM_KEY = os.environ.get("AISSTREAM_API_KEY", "")
CARTO_KEY = os.environ.get("CARTO_API_KEY", "")
SYNC_TOKEN = os.environ.get("SYNC_TOKEN", "ost_sync_99e8a71b26f50c3d")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
DB_FILE = os.path.join(DATA_DIR, "bight.db")

# Cloud Sync Settings (for pushing from collector to central server)
CLOUD_SYNC_ENABLED = os.environ.get("CLOUD_SYNC_ENABLED", "false").lower() in ("true", "1", "yes")
CLOUD_SYNC_URL = os.environ.get("CLOUD_SYNC_URL", "https://open-ship-tracker.fly.dev/api/sync/batch")
CLOUD_SYNC_TOKEN = os.environ.get("CLOUD_SYNC_TOKEN", SYNC_TOKEN)
CLOUD_SYNC_INTERVAL = int(os.environ.get("CLOUD_SYNC_INTERVAL", "30"))

sync_queue_positions: List[Dict[str, Any]] = []
sync_queue_statics: List[Dict[str, Any]] = []

# Bounding Box: Deutsche Bucht + Elbe/Weser/NOK + westliche Ostsee (Kiel bis Rügen)
# Format für AISStream: [[[lat_min, lon_min], [lat_max, lon_max]]]
DEFAULT_BBOX = [[[53.10, 5.80], [55.35, 14.30]]]

app = FastAPI(
    title="OpenShipTracker – AIS Live Radar, Fleet Filter & MMSI Database",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NAV_STATUS_MAP = {
    0: "Unter Maschine",
    1: "Vor Anker",
    2: "Manövrierunfähig",
    3: "Manövrierbehindert",
    4: "Tiefgangsbehindert",
    5: "Festgemacht",
    6: "Auf Grund",
    7: "Fischerei",
    8: "Unter Segel",
    14: "AIS-SART / Notfall",
    15: "Aktiv (AIS)"
}

MID_COUNTRY_MAP = {
    "211": "DE", "218": "DE",
    "219": "DK", "220": "DK",
    "244": "NL", "245": "NL", "246": "NL",
    "205": "BE",
    "232": "GB", "233": "GB", "234": "GB", "235": "GB",
    "257": "NO", "258": "NO", "259": "NO",
    "265": "SE", "266": "SE",
    "230": "FI",
    "261": "PL",
    "226": "FR", "227": "FR", "228": "FR",
    "247": "IT",
    "224": "ES", "225": "ES",
    "255": "PT",
    "209": "CY", "210": "CY", "212": "CY",
    "215": "MT", "229": "MT", "248": "MT", "249": "MT", "256": "MT",
    "237": "GR", "239": "GR", "240": "GR", "241": "GR",
    "273": "RU",
    "275": "LV",
    "276": "EE",
    "277": "LT",
    "304": "AG", "305": "AG",
    "308": "BS", "309": "BS", "311": "BS",
    "314": "BB",
    "319": "KY",
    "351": "PA", "352": "PA", "353": "PA", "354": "PA", "355": "PA", "356": "PA", "357": "PA", "370": "PA", "371": "PA", "372": "PA", "373": "PA", "374": "PA",
    "338": "US", "366": "US", "367": "US", "368": "US", "369": "US",
    "477": "HK",
    "538": "MH",
    "563": "SG", "564": "SG", "565": "SG", "566": "SG",
    "636": "LR", "637": "LR"
}

stream_state = {
    "connected": False,
    "messages_received": 0,
    "positions_archived": 0,
    "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "last_message_at": None,
    "retention_days": 30,
    "bbox": DEFAULT_BBOX,
    "reconnect_requested": False
}

# In-memory cache of last archived point per MMSI to downsample history efficiently:
# { mmsi: (lat, lon, epoch_sec, sog, cog) }
last_archived_cache: Dict[str, tuple] = {}

DEFAULT_NAVY_VESSELS = [
    {"mmsi": "211210160", "name": "Brandenburg", "pennant": "F 215", "category": "Fregatte", "class_name": "Klasse 123 (Brandenburg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210170", "name": "Schleswig-Holstein", "pennant": "F 216", "category": "Fregatte", "class_name": "Klasse 123 (Brandenburg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210180", "name": "Bayern", "pennant": "F 217", "category": "Fregatte", "class_name": "Klasse 123 (Brandenburg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210190", "name": "Mecklenburg-Vorpommern", "pennant": "F 218", "category": "Fregatte", "class_name": "Klasse 123 (Brandenburg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210150", "name": "Sachsen", "pennant": "F 219", "category": "Fregatte", "class_name": "Klasse 124 (Sachsen)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211905000", "name": "Hamburg", "pennant": "F 220", "category": "Fregatte", "class_name": "Klasse 124 (Sachsen)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210200", "name": "Augsburg (a.D.)", "pennant": "F 213", "category": "Fregatte", "class_name": "Klasse 122 (Bremen)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211906000", "name": "Hessen", "pennant": "F 221", "category": "Fregatte", "class_name": "Klasse 124 (Sachsen)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211920000", "name": "Baden-Württemberg", "pennant": "F 222", "category": "Fregatte", "class_name": "Klasse 125 (Baden-Württemberg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211921000", "name": "Nordrhein-Westfalen", "pennant": "F 223", "category": "Fregatte", "class_name": "Klasse 125 (Baden-Württemberg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211922000", "name": "Sachsen-Anhalt", "pennant": "F 224", "category": "Fregatte", "class_name": "Klasse 125 (Baden-Württemberg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211923000", "name": "Rheinland-Pfalz", "pennant": "F 225", "category": "Fregatte", "class_name": "Klasse 125 (Baden-Württemberg)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211910000", "name": "Braunschweig", "pennant": "F 260", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig)", "homeport": "Warnemünde"},
    {"mmsi": "211911000", "name": "Magdeburg", "pennant": "F 261", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig)", "homeport": "Warnemünde"},
    {"mmsi": "211912000", "name": "Erfurt", "pennant": "F 262", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig)", "homeport": "Warnemünde"},
    {"mmsi": "211913000", "name": "Oldenburg", "pennant": "F 263", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig)", "homeport": "Warnemünde"},
    {"mmsi": "211914000", "name": "Ludwigshafen am Rhein", "pennant": "F 264", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig)", "homeport": "Warnemünde"},
    {"mmsi": "211832000", "name": "Köln", "pennant": "F 265", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig 2. Los)", "homeport": "Warnemünde"},
    {"mmsi": "211847000", "name": "Emden", "pennant": "F 266", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig 2. Los)", "homeport": "Warnemünde"},
    {"mmsi": "211848000", "name": "Karlsruhe", "pennant": "F 267", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig 2. Los)", "homeport": "Warnemünde"},
    {"mmsi": "211849000", "name": "Augsburg", "pennant": "F 268", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig 2. Los)", "homeport": "Warnemünde"},
    {"mmsi": "211850000", "name": "Lübeck", "pennant": "F 269", "category": "Korvette", "class_name": "Klasse 130 (Braunschweig 2. Los)", "homeport": "Warnemünde"},
    {"mmsi": "211917000", "name": "Karlsruhe (2)", "pennant": "F 267", "category": "Korvette", "class_name": "Klasse 130 Batch 2", "homeport": "Warnemünde"},
    {"mmsi": "211918000", "name": "Augsburg (2)", "pennant": "F 268", "category": "Korvette", "class_name": "Klasse 130 Batch 2", "homeport": "Warnemünde"},
    {"mmsi": "211919000", "name": "Lübeck (2)", "pennant": "F 269", "category": "Korvette", "class_name": "Klasse 130 Batch 2", "homeport": "Warnemünde"},
    {"mmsi": "211901000", "name": "U 31", "pennant": "S 181", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211902000", "name": "U 32", "pennant": "S 182", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211903000", "name": "U 33", "pennant": "S 183", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211904000", "name": "U 34", "pennant": "S 184", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211905000", "name": "U 35", "pennant": "S 185", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211907000", "name": "U 36", "pennant": "S 186", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211211730", "name": "Berlin", "pennant": "A 1411", "category": "EGV", "class_name": "Klasse 702 (Berlin)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210420", "name": "Frankfurt am Main", "pennant": "A 1412", "category": "EGV", "class_name": "Klasse 702 (Berlin)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211210430", "name": "Bonn", "pennant": "A 1413", "category": "EGV", "class_name": "Klasse 702 (Berlin)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211927000", "name": "Bonn (2)", "pennant": "A 1413", "category": "EGV", "class_name": "Klasse 702 (Berlin)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211211470", "name": "Alster", "pennant": "A 50", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211480", "name": "Oste", "pennant": "A 52", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211490", "name": "Oker", "pennant": "A 53", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211310", "name": "Alster (2)", "pennant": "A 50", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211320", "name": "Oste (2)", "pennant": "A 52", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211330", "name": "Oker (2)", "pennant": "A 53", "category": "Flottendienstboot", "class_name": "Klasse 423 (Oste)", "homeport": "Eckernförde"},
    {"mmsi": "211211410", "name": "Elbe", "pennant": "A 511", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Warnemünde"},
    {"mmsi": "211211420", "name": "Mosel", "pennant": "A 512", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Kiel"},
    {"mmsi": "211211430", "name": "Rhein", "pennant": "A 513", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Kiel"},
    {"mmsi": "211211440", "name": "Werra", "pennant": "A 514", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Kiel"},
    {"mmsi": "211211450", "name": "Main", "pennant": "A 515", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Eckernförde"},
    {"mmsi": "211211460", "name": "Donau", "pennant": "A 516", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Warnemünde"},
    {"mmsi": "211211530", "name": "Main (2)", "pennant": "A 515", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Eckernförde"},
    {"mmsi": "211211540", "name": "Donau (2)", "pennant": "A 516", "category": "Tender", "class_name": "Klasse 404 (Elbe)", "homeport": "Warnemünde"},
    {"mmsi": "211211710", "name": "Rottweil", "pennant": "M 1061", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211720", "name": "Sulzbach-Rosenberg", "pennant": "M 1062", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211740", "name": "Bad Bevensen", "pennant": "M 1063", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211750", "name": "Grömitz", "pennant": "M 1064", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211760", "name": "Dillingen", "pennant": "M 1065", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211770", "name": "Bad Rappenau", "pennant": "M 1067", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211780", "name": "Datteln", "pennant": "M 1068", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211790", "name": "Homburg", "pennant": "M 1069", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211810", "name": "Pegnitz", "pennant": "M 1090", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211820", "name": "Siegburg", "pennant": "M 1098", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211150", "name": "Fulda", "pennant": "M 1058", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211160", "name": "Weilheim", "pennant": "M 1059", "category": "Minenjagdboot", "class_name": "Klasse 332 (Frankenthal)", "homeport": "Kiel"},
    {"mmsi": "211211510", "name": "Spessart", "pennant": "A 1442", "category": "Versorger", "class_name": "Klasse 704 (Rhön)", "homeport": "Kiel"},
    {"mmsi": "211211520", "name": "Rhön", "pennant": "A 1443", "category": "Versorger", "class_name": "Klasse 704 (Rhön)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211211620", "name": "Spessart (2)", "pennant": "A 1442", "category": "Versorger", "class_name": "Klasse 704 (Rhön)", "homeport": "Kiel"},
    {"mmsi": "211211630", "name": "Rhön (2)", "pennant": "A 1443", "category": "Versorger", "class_name": "Klasse 704 (Rhön)", "homeport": "Wilhelmshaven"},
    {"mmsi": "211211600", "name": "Planet", "pennant": "A 1458", "category": "Forschungsschiff", "class_name": "Klasse 751 (Planet)", "homeport": "Eckernförde"},
    {"mmsi": "211915000", "name": "Rügen", "pennant": "A 1459", "category": "Schlepper", "class_name": "Bergungsschlepper", "homeport": "Kiel"},
    {"mmsi": "211916000", "name": "Borkum", "pennant": "Y 890", "category": "Schlepper", "class_name": "Hafenschlepper", "homeport": "Kiel"},
    {"mmsi": "211210280", "name": "Gorch Fock", "pennant": "A 60", "category": "Schulschiff", "class_name": "Gorch-Fock-Klasse", "homeport": "Kiel"},
    {"mmsi": "211930000", "name": "U 35", "pennant": "S 185", "category": "U-Boot", "class_name": "Klasse 212A", "homeport": "Eckernförde"},
    {"mmsi": "211212720", "name": "Munster", "pennant": "A 1433", "category": "Unterstützungseinheit", "class_name": "Klasse 740 (Mehrzweckboot)", "homeport": "Eckernförde"},
    {"mmsi": "211212700", "name": "Baumholder", "pennant": "A 1434", "category": "Unterstützungseinheit", "class_name": "Klasse 740 (Mehrzweckboot)", "homeport": "Eckernförde"}
]
NAVY_MMSI_LOOKUP = {v["mmsi"]: v for v in DEFAULT_NAVY_VESSELS}

class FleetCreateRequest(BaseModel):
    name: str
    icon: Optional[str] = "⭐"
    color: Optional[str] = "#f59e0b"

class FleetVesselRequest(BaseModel):
    mmsi: str
    custom_name: Optional[str] = None
    notes: Optional[str] = None

class SettingsUpdate(BaseModel):
    retention_days: int = 30
    aisstream_key: Optional[str] = None
    datadocked_key: Optional[str] = None
    carto_key: Optional[str] = None
    lat_min: Optional[float] = None
    lon_min: Optional[float] = None
    lat_max: Optional[float] = None
    lon_max: Optional[float] = None
    bbox: Optional[List[List[List[float]]]] = None
    allow_registration: Optional[bool] = None

class RegisterRequest(BaseModel):
    username: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class UpdateRoleRequest(BaseModel):
    role: str

def get_country_from_mmsi(mmsi: str) -> str:
    if len(mmsi) == 9:
        return MID_COUNTRY_MAP.get(mmsi[:3], mmsi[:3])
    return "-"

def classify_ship_type(type_code: Optional[int], mmsi: str = "", name: str = "") -> tuple[str, str]:
    """Gibt (Kategorie-Schlüssel, Anzeigename) anhand des AIS ShipType-Codes oder der MMSI zurück."""
    tc = int(type_code) if type_code is not None else 0
    upper_name = (name or "").upper()

    if mmsi in NAVY_MMSI_LOOKUP:
        nv = NAVY_MMSI_LOOKUP[mmsi]
        return ("Military", f"Deutsche Marine ({nv['category']})")

    # Passagierschiffe (AIS Typ 60-69)
    # Wichtig: "FGS " steht in der deutschen Binnenschifffahrt für "Fahrgastschiff" (z. B. FGS HAMBURG auf der Spree).
    # Zivile Fahrgastschiffe dürfen niemals als Kriegsschiffe klassifiziert werden!
    if 60 <= tc <= 69 or (upper_name.startswith("FGS ") and not (mmsi.startswith("2119") or mmsi.startswith("21121"))):
        return ("Passenger", "Passagierschiff")

    if tc == 35 or mmsi.startswith("2119") or mmsi.startswith("21121") or "WARSHIP" in upper_name:
        return ("Military", "Militär / Marine")
    if tc == 55 or "POLIZEI" in upper_name or "ZOLL" in upper_name or "COAST GUARD" in upper_name or "BP " in upper_name:
        return ("LawEnforcement", "Behörde / Küstenwache")
    if tc == 51 or "SAR" in upper_name or "RESCUE" in upper_name:
        return ("SAR", "Seenotrettung (SAR)")
    if tc == 52 or tc == 31 or tc == 32:
        return ("Tug", "Schlepper")
    if tc == 50:
        return ("Pilot", "Lotse")
    if tc == 30:
        return ("Fishing", "Fischerei")
    if tc == 36:
        return ("Sailing", "Segelschiff")
    if tc == 37:
        return ("Pleasure", "Sportboot / Yacht")
    if 40 <= tc <= 49:
        return ("HSC", "Schnellfähre (HSC)")
    if 60 <= tc <= 69:
        return ("Passenger", "Passagierschiff / Fähre")
    if 70 <= tc <= 79:
        return ("Cargo", "Frachtschiff")
    if 80 <= tc <= 89:
        return ("Tanker", "Tanker")
    if tc in (33, 34, 53, 54, 58):
        return ("Special", "Spezial- / Arbeitsschiff")
    return ("Other", "Sonstige / Unbekannt")

def normalize_iso_timestamp(ts_val) -> str:
    if not ts_val:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(ts_val).strip()
    m = re.match(r'^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:\s*\+00:?00(?:\s*UTC)?|Z)?$', s)
    if m:
        return f"{m.group(1)}T{m.group(2)}Z"
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * R * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

def load_config() -> Dict[str, Any]:
    default_cfg = {
        "retention_days": 30,
        "aisstream_key": AISSTREAM_KEY,
        "carto_key": CARTO_KEY,
        "bbox": DEFAULT_BBOX,
        "allow_registration": True
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CONFIG_FILE):
        save_config(default_cfg)
        return default_cfg
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            for k, v in default_cfg.items():
                if k not in cfg:
                    cfg[k] = v
            return cfg
    except:
        return default_cfg

def save_config(cfg: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

SESSION_COOKIE_NAME = "ost_session_token"
SESSION_DURATION_DAYS = 30

def hash_password(password: str) -> Tuple[str, str]:
    salt = secrets.token_hex(16)
    pw_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200_000).hex()
    return pw_hash, salt

def verify_password(password: str, pw_hash: str, salt: str) -> bool:
    check = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200_000).hex()
    return secrets.compare_digest(check, pw_hash)

def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=SESSION_DURATION_DAYS)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO user_sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, user_id, now.isoformat(), expires.isoformat())
        )
    return token

def get_user_from_session(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT u.id, u.username, u.role, u.is_active, s.expires_at
                FROM user_sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.token = ? AND s.expires_at > ? AND u.is_active = 1
            """, (token, now_iso))
            row = cur.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Error querying session: {e}")
    return None

def delete_session(token: str):
    if not token:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("DELETE FROM user_sessions WHERE token = ?", (token,))
    except Exception as e:
        logger.error(f"Error deleting session: {e}")

def get_current_user(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()
    return get_user_from_session(token)

def require_auth(request: Request) -> Dict[str, Any]:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentifizierung erforderlich. Bitte einloggen.")
    return user

def require_admin(request: Request) -> Dict[str, Any]:
    user = require_auth(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Administratorrechte erforderlich.")
    return user

def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Check for seed database if volume is fresh
    if not os.path.exists(DB_FILE):
        seed_candidates = [
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "seed_bight.db"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bight.db"),
            "/app/seed_bight.db"
        ]
        for sc in seed_candidates:
            if os.path.exists(sc) and os.path.getsize(sc) > 1000:
                try:
                    shutil.copy2(sc, DB_FILE)
                    logger.info(f"Initialized database from seed file: {sc}")
                    break
                except Exception as seed_err:
                    logger.error(f"Error copying seed DB from {sc}: {seed_err}")

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vessels (
                mmsi TEXT PRIMARY KEY,
                name TEXT,
                callsign TEXT,
                imo TEXT,
                ship_type INTEGER,
                category TEXT,
                category_label TEXT,
                country TEXT,
                length REAL,
                width REAL,
                draught REAL,
                destination TEXT,
                eta TEXT,
                latitude REAL,
                longitude REAL,
                speed REAL,
                course REAL,
                heading REAL,
                nav_status TEXT,
                first_seen TEXT,
                last_seen TEXT,
                position_count INTEGER DEFAULT 0
            )
        """)
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
                timestamp TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_mmsi_time ON position_history(mmsi, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_time ON position_history(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vessels_last_seen ON vessels(last_seen)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vessels_category ON vessels(category)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fleets (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                icon TEXT DEFAULT '⭐',
                color TEXT DEFAULT '#3b82f6',
                is_system INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fleet_vessels (
                fleet_id TEXT NOT NULL,
                mmsi TEXT NOT NULL,
                custom_name TEXT,
                notes TEXT,
                PRIMARY KEY (fleet_id, mmsi)
            )
        """)

        # Benutzer & Session-Verwaltung
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_login TEXT,
                email TEXT
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Seed Standard-Flotten
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO fleets (id, name, icon, color, is_system) VALUES ('navy', 'Deutsche Marine', '⚓', '#3b82f6', 1)")
        cur.execute("INSERT OR IGNORE INTO fleets (id, name, icon, color, is_system) VALUES ('my_fleet', 'Meine Flotte', '⭐', '#f59e0b', 1)")
        cur.execute("INSERT OR IGNORE INTO fleets (id, name, icon, color, is_system) VALUES ('sar_authorities', 'SAR & Küstenwache', '🛟', '#10b981', 1)")

        for nv in DEFAULT_NAVY_VESSELS:
            label = f"{nv['name']} ({nv['pennant']})" if nv.get("pennant") else nv["name"]
            cur.execute(
                "INSERT OR IGNORE INTO fleet_vessels (fleet_id, mmsi, custom_name, notes) VALUES (?, ?, ?, ?)",
                ("navy", nv["mmsi"], label, f"{nv.get('category', '')} • {nv.get('homeport', '')}")
            )
            # Falls das Schiff schon in vessels ist, aber noch 'MMSI ...' heißt, direkt mit Klarnamen veredeln
            cur.execute(
                "UPDATE vessels SET name = ?, category = 'Military', category_label = ? WHERE mmsi = ? AND (name IS NULL OR name LIKE 'MMSI %')",
                (label, f"Deutsche Marine ({nv['category']})", nv["mmsi"])
            )
        conn.commit()

def should_archive_point(mmsi: str, lat: float, lon: float, sog: float, cog: float, now_epoch: float) -> bool:
    """Intelligentes Downsampling für 30 Tage Speicherung tausender Schiffe."""
    prev = last_archived_cache.get(mmsi)
    if not prev:
        last_archived_cache[mmsi] = (lat, lon, now_epoch, sog, cog)
        return True

    p_lat, p_lon, p_epoch, p_sog, p_cog = prev
    dt = now_epoch - p_epoch
    if dt < 45:
        return False

    dist_m = haversine_m(p_lat, p_lon, lat, lon)
    cog_diff = abs((cog or 0.0) - (p_cog or 0.0))
    if cog_diff > 180:
        cog_diff = 360 - cog_diff

    # Wenn Schiff liegt (< 0.5 kn) und sich kaum bewegt (< 80m): alle 20 Minuten 1 Punkt speichern
    if (sog or 0.0) < 0.5 and dist_m < 80:
        if dt >= 1200:
            last_archived_cache[mmsi] = (lat, lon, now_epoch, sog, cog)
            return True
        return False

    # Wenn in Fahrt: speichern wenn >= 180s vergangen ODER (>= 60s und Kursänderung >= 12° oder Distanz >= 350m)
    if dt >= 180 or (dt >= 60 and (cog_diff >= 12.0 or dist_m >= 350.0)):
        last_archived_cache[mmsi] = (lat, lon, now_epoch, sog, cog)
        return True

    return False

async def aisstream_worker():
    """Verbindet sich mit AISStream.io für die gesamte Deutsche Bucht + Elbe/Weser/NOK + westliche Ostsee."""
    while True:
        cfg = load_config()
        key = cfg.get("aisstream_key") or AISSTREAM_KEY
        bbox = cfg.get("bbox") or DEFAULT_BBOX
        stream_state["retention_days"] = cfg.get("retention_days", 30)
        stream_state["bbox"] = bbox
        stream_state["reconnect_requested"] = False

        if not key:
            stream_state["connected"] = False
            await asyncio.sleep(10)
            continue

        url = "wss://stream.aisstream.io/v0/stream"
        logger.info(f"Connecting to AISStream.io for BBox {bbox}...")

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=2048) as ws:
                stream_state["connected"] = True
                sub = {
                    "APIKey": key,
                    "BoundingBoxes": bbox,
                    "FilterMessageTypes": [
                        "PositionReport",
                        "StandardClassBPositionReport",
                        "ExtendedClassBPositionReport",
                        "ShipStaticData",
                        "StaticDataReport"
                    ]
                }
                await ws.send(json.dumps(sub))
                logger.info("AISStream regional subscription active!")

                batch_positions = []
                batch_statics = []
                last_flush = time.time()

                async for raw in ws:
                    if stream_state.get("reconnect_requested"):
                        logger.info("BBox or settings changed -> reconnecting AISStream...")
                        flush_batches(batch_positions, batch_statics)
                        break
                    try:
                        msg = json.loads(raw)
                        m_type = msg.get("MessageType")
                        meta = msg.get("MetaData", {})
                        mmsi = str(meta.get("MMSI") or "").strip()
                        if not mmsi or len(mmsi) < 7:
                            continue

                        ship_name = (meta.get("ShipName") or "").strip().strip("@")
                        iso_ts = normalize_iso_timestamp(meta.get("time_utc"))
                        now_epoch = time.time()

                        stream_state["messages_received"] += 1
                        stream_state["last_message_at"] = iso_ts

                        message_body = msg.get("Message", {}).get(m_type, {})

                        if m_type in ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"):
                            lat = message_body.get("Latitude", meta.get("latitude"))
                            lon = message_body.get("Longitude", meta.get("longitude"))
                            if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180 or (lat == 0 and lon == 0):
                                continue

                            sog = float(message_body.get("Sog") or 0.0)
                            cog = float(message_body.get("Cog") or 0.0)
                            hdg = message_body.get("TrueHeading")
                            if hdg is None or hdg >= 360:
                                hdg = cog
                            else:
                                hdg = float(hdg)

                            nav_code = message_body.get("NavigationalStatus", 15)
                            nav_text = NAV_STATUS_MAP.get(nav_code, "Aktiv (AIS)")

                            ship_type_ext = message_body.get("Type")
                            archive_flag = should_archive_point(mmsi, float(lat), float(lon), sog, cog, now_epoch)

                            batch_positions.append({
                                "mmsi": mmsi,
                                "name": ship_name,
                                "lat": round(float(lat), 6),
                                "lon": round(float(lon), 6),
                                "sog": round(sog, 1),
                                "cog": round(cog, 1),
                                "hdg": round(hdg, 1),
                                "nav_status": nav_text,
                                "ship_type": ship_type_ext,
                                "ts": iso_ts,
                                "archive": archive_flag
                            })

                        elif m_type == "ShipStaticData":
                            callsign = (message_body.get("CallSign") or "").strip().strip("@")
                            dest = (message_body.get("Destination") or "").strip().strip("@")
                            imo = str(message_body.get("ImoNumber") or "")
                            if imo == "0":
                                imo = ""
                            ship_type = message_body.get("Type")
                            draught = message_body.get("MaximumStaticDraught")
                            dim = message_body.get("Dimension") or {}
                            length = (dim.get("A") or 0) + (dim.get("B") or 0)
                            width = (dim.get("C") or 0) + (dim.get("D") or 0)
                            s_name = (message_body.get("Name") or ship_name or "").strip().strip("@")

                            batch_statics.append({
                                "mmsi": mmsi,
                                "name": s_name,
                                "callsign": callsign,
                                "imo": imo,
                                "ship_type": ship_type,
                                "length": length if length > 0 else None,
                                "width": width if width > 0 else None,
                                "draught": draught if draught and draught > 0 else None,
                                "destination": dest,
                                "ts": iso_ts
                            })

                        elif m_type == "StaticDataReport":
                            rep_a = message_body.get("ReportA", {})
                            rep_b = message_body.get("ReportB", {})
                            s_name = (rep_a.get("Name") or ship_name or "").strip().strip("@")
                            callsign = (rep_b.get("CallSign") or "").strip().strip("@")
                            ship_type = rep_b.get("ShipType")
                            dim = rep_b.get("Dimension") or {}
                            length = (dim.get("A") or 0) + (dim.get("B") or 0)
                            width = (dim.get("C") or 0) + (dim.get("D") or 0)

                            batch_statics.append({
                                "mmsi": mmsi,
                                "name": s_name,
                                "callsign": callsign,
                                "imo": None,
                                "ship_type": ship_type,
                                "length": length if length > 0 else None,
                                "width": width if width > 0 else None,
                                "draught": None,
                                "destination": None,
                                "ts": iso_ts
                            })

                        # Flush batch to SQLite every 1.5 seconds or 80 items
                        if (time.time() - last_flush) >= 1.5 or (len(batch_positions) + len(batch_statics)) >= 80:
                            flush_batches(batch_positions, batch_statics)
                            batch_positions.clear()
                            batch_statics.clear()
                            last_flush = time.time()

                    except Exception as parse_err:
                        logger.warning(f"Parse error: {parse_err}")

        except Exception as conn_err:
            stream_state["connected"] = False
            logger.warning(f"AISStream disconnected ({conn_err}). Reconnecting in 5s...")
            await asyncio.sleep(5)

def flush_batches(positions: List[Dict[str, Any]], statics: List[Dict[str, Any]]):
    if not positions and not statics:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            archived_now = 0

            for p in positions:
                mmsi = p["mmsi"]
                country = get_country_from_mmsi(mmsi)
                nv = NAVY_MMSI_LOOKUP.get(mmsi)
                fallback_name = f"{nv['name']} ({nv['pennant']})" if nv and nv.get("pennant") else (nv["name"] if nv else f"MMSI {mmsi}")
                cat, cat_label = classify_ship_type(p.get("ship_type"), mmsi, p.get("name") or fallback_name)
                inc = 1 if p["archive"] else 0

                cur.execute("SELECT name, ship_type, category, category_label, position_count FROM vessels WHERE mmsi = ?", (mmsi,))
                row = cur.fetchone()
                if not row:
                    cur.execute("""
                        INSERT INTO vessels (
                            mmsi, name, ship_type, category, category_label, country,
                            latitude, longitude, speed, course, heading, nav_status,
                            first_seen, last_seen, position_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        mmsi, p["name"] or fallback_name, p.get("ship_type"), cat, cat_label, country,
                        p["lat"], p["lon"], p["sog"], p["cog"], p["hdg"], p["nav_status"],
                        p["ts"], p["ts"], inc
                    ))
                else:
                    ex_name, ex_type, ex_cat, ex_cat_label, ex_count = row
                    if p["name"]:
                        new_name = p["name"]
                    elif ex_name and not ex_name.startswith("MMSI "):
                        new_name = ex_name
                    else:
                        new_name = fallback_name
                    new_type = p["ship_type"] if p.get("ship_type") else ex_type
                    if (new_type and (not ex_cat or ex_cat == "Other")) or nv:
                        new_cat, new_cat_label = classify_ship_type(new_type, mmsi, new_name or "")
                    else:
                        new_cat, new_cat_label = ex_cat or cat, ex_cat_label or cat_label

                    cur.execute("""
                        UPDATE vessels SET
                            name = ?,
                            ship_type = ?,
                            category = ?,
                            category_label = ?,
                            latitude = ?,
                            longitude = ?,
                            speed = ?,
                            course = ?,
                            heading = ?,
                            nav_status = ?,
                            last_seen = ?,
                            position_count = ?
                        WHERE mmsi = ?
                    """, (
                        new_name, new_type, new_cat, new_cat_label,
                        p["lat"], p["lon"], p["sog"], p["cog"], p["hdg"], p["nav_status"],
                        p["ts"], (ex_count or 0) + inc, mmsi
                    ))

                if p["archive"]:
                    cur.execute("""
                        INSERT INTO position_history (mmsi, latitude, longitude, speed, course, heading, status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (mmsi, p["lat"], p["lon"], p["sog"], p["cog"], p["hdg"], p["nav_status"], p["ts"]))
                    archived_now += 1

            for s in statics:
                mmsi = s["mmsi"]
                country = get_country_from_mmsi(mmsi)
                cat, cat_label = classify_ship_type(s.get("ship_type"), mmsi, s.get("name") or "")

                cur.execute("SELECT name, callsign, imo, ship_type, length, width, draught, destination FROM vessels WHERE mmsi = ?", (mmsi,))
                row = cur.fetchone()
                if not row:
                    cur.execute("""
                        INSERT INTO vessels (
                            mmsi, name, callsign, imo, ship_type, category, category_label, country,
                            length, width, draught, destination, first_seen, last_seen, position_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        mmsi, s["name"] or f"MMSI {mmsi}", s["callsign"], s["imo"],
                        s["ship_type"], cat, cat_label, country,
                        s["length"], s["width"], s["draught"], s["destination"],
                        s["ts"], s["ts"]
                    ))
                else:
                    ex_name, ex_call, ex_imo, ex_type, ex_len, ex_wid, ex_dr, ex_dest = row
                    new_name = s["name"] or ex_name
                    new_call = s["callsign"] or ex_call
                    new_imo = s["imo"] or ex_imo
                    new_type = s["ship_type"] if s["ship_type"] else ex_type
                    new_cat, new_cat_label = classify_ship_type(new_type, mmsi, new_name or "")
                    new_len = s["length"] or ex_len
                    new_wid = s["width"] or ex_wid
                    new_dr = s["draught"] or ex_dr
                    new_dest = s["destination"] if s["destination"] else ex_dest

                    cur.execute("""
                        UPDATE vessels SET
                            name = ?, callsign = ?, imo = ?, ship_type = ?,
                            category = ?, category_label = ?, country = ?,
                            length = ?, width = ?, draught = ?, destination = ?,
                            last_seen = ?
                        WHERE mmsi = ?
                    """, (
                        new_name, new_call, new_imo, new_type,
                        new_cat, new_cat_label, country,
                        new_len, new_wid, new_dr, new_dest,
                        s["ts"], mmsi
                    ))

            conn.commit()
            stream_state["positions_archived"] += archived_now

            if CLOUD_SYNC_ENABLED:
                if len(sync_queue_positions) < 5000:
                    sync_queue_positions.extend(positions)
                if len(sync_queue_statics) < 1000:
                    sync_queue_statics.extend(statics)
    except Exception as e:
        logger.error(f"Error flushing batches to SQLite: {e}")

async def retention_cleanup_worker():
    """Löscht automatisch alle Positionsdaten aus position_history, die älter als retention_days (Standard: 30 Tage) sind,
    behält aber von jedem Schiff mindestens den allerneuesten Punkt dauerhaft bei!
    Die MMSI-Stammdatenbank in 'vessels' bleibt ebenfalls dauerhaft erhalten."""
    while True:
        try:
            cfg = load_config()
            days = max(1, int(cfg.get("retention_days", 30)))
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            with sqlite3.connect(DB_FILE) as conn:
                cur = conn.cursor()
                cur.execute("""
                    DELETE FROM position_history
                    WHERE timestamp < ?
                      AND id NOT IN (SELECT MAX(id) FROM position_history GROUP BY mmsi)
                """, (cutoff,))
                deleted = cur.rowcount
                conn.commit()
                if deleted > 0:
                    logger.info(f"30-Day Retention Cleanup: Removed {deleted} old track points before {cutoff} (kept latest point per vessel)")
        except Exception as e:
            logger.error(f"Retention cleanup error: {e}")
        await asyncio.sleep(3600)  # Einmal pro Stunde prüfen

async def cloud_sync_worker():
    """Pusht periodisch (z. B. alle 30s) neue Deltas an den zentralen Cloud-Server (Fly.io)."""
    while True:
        await asyncio.sleep(CLOUD_SYNC_INTERVAL)
        if not CLOUD_SYNC_ENABLED or not CLOUD_SYNC_URL:
            continue
        if not sync_queue_positions and not sync_queue_statics:
            continue

        pos_batch = sync_queue_positions[:500]
        stat_batch = sync_queue_statics[:200]

        payload = {
            "token": CLOUD_SYNC_TOKEN,
            "positions": pos_batch,
            "statics": stat_batch
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                res = await client.post(CLOUD_SYNC_URL, json=payload)
                if res.status_code == 200:
                    del sync_queue_positions[:len(pos_batch)]
                    del sync_queue_statics[:len(stat_batch)]
                    logger.info(f"Cloud-Sync OK: {len(pos_batch)} Pos, {len(stat_batch)} Stat an {CLOUD_SYNC_URL} übertragen")
                else:
                    logger.warning(f"Cloud-Sync HTTP {res.status_code}: {res.text[:120]}")
        except Exception as e:
            logger.warning(f"Cloud-Sync Verbindungsfehler ({e})")

# --- AUTHENTICATION & USER MANAGEMENT ENDPOINTS ---

@app.get("/api/auth/me")
async def auth_me(request: Request):
    user = get_current_user(request)
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        user_count = cur.fetchone()[0]
    cfg = load_config()
    reg_enabled = cfg.get("allow_registration", True)
    return {
        "authenticated": user is not None,
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"]
        } if user else None,
        "has_users": user_count > 0,
        "registration_enabled": bool(reg_enabled or user_count == 0)
    }

@app.post("/api/auth/register")
async def auth_register(req: RegisterRequest, response: Response):
    username = req.username.strip()
    password = req.password
    if len(username) < 3:
        raise HTTPException(status_code=400, detail="Der Benutzername muss mindestens 3 Zeichen lang sein.")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Das Passwort muss mindestens 6 Zeichen lang sein.")

    cfg = load_config()
    allow_reg = cfg.get("allow_registration", True)

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        user_count = cur.fetchone()[0]

        if user_count > 0 and not allow_reg:
            raise HTTPException(status_code=403, detail="Die Registrierung für neue Konten ist derzeit deaktiviert.")

        cur.execute("SELECT id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Dieser Benutzername ist bereits vergeben.")

        # Der erste Account wird automatisch Admin!
        role = "admin" if user_count == 0 else "user"
        pw_hash, salt = hash_password(password)
        now_iso = datetime.now(timezone.utc).isoformat()

        cur.execute("""
            INSERT INTO users (username, email, password_hash, salt, role, created_at, last_login)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (username, username, pw_hash, salt, role, now_iso, now_iso))
        user_id = cur.lastrowid

    token = create_session(user_id)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_DURATION_DAYS * 86400,
        httponly=True,
        samesite="lax",
        secure=False
    )
    return {
        "status": "ok",
        "user": {
            "id": user_id,
            "username": username,
            "role": role
        }
    }

@app.post("/api/auth/login")
async def auth_login(req: LoginRequest, response: Response):
    username = req.username.strip()
    password = req.password
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM users 
            WHERE (LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?)) 
              AND is_active = 1
        """, (username, username))
        user = cur.fetchone()
        if not user or not verify_password(password, user["password_hash"], user["salt"]):
            raise HTTPException(status_code=401, detail="Ungültiger Benutzername oder falsches Passwort.")

        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE users SET last_login = ? WHERE id = ?", (now_iso, user["id"]))

    token = create_session(user["id"])
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_DURATION_DAYS * 86400,
        httponly=True,
        samesite="lax",
        secure=False
    )
    return {
        "status": "ok",
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"]
        }
    }

@app.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        delete_session(token)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return {"status": "ok"}

@app.get("/api/auth/users")
async def get_users_list(request: Request):
    require_admin(request)
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, username, role, is_active, created_at, last_login FROM users ORDER BY id ASC")
        return {"users": [dict(r) for r in cur.fetchall()]}

@app.post("/api/auth/users/{user_id}/role")
async def update_user_role(user_id: int, req: UpdateRoleRequest, request: Request):
    admin = require_admin(request)
    new_role = req.role.strip().lower()
    if new_role not in ["admin", "user"]:
        raise HTTPException(status_code=400, detail="Ungültige Rolle. Nur 'admin' oder 'user' erlaubt.")
    if admin["id"] == user_id and new_role != "admin":
        raise HTTPException(status_code=400, detail="Sie können sich Ihre eigenen Administratorrechte nicht entziehen.")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (new_role, user_id))
    return {"status": "ok", "user_id": user_id, "role": new_role}

@app.delete("/api/auth/users/{user_id}")
async def delete_user_account(user_id: int, request: Request):
    admin = require_admin(request)
    if admin["id"] == user_id:
        raise HTTPException(status_code=400, detail="Sie können Ihr eigenes Konto nicht löschen.")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    return {"status": "ok", "deleted_user_id": user_id}

# --- API ENDPOINTS ---

class BatchSyncRequest(BaseModel):
    token: str
    positions: List[Dict[str, Any]] = []
    statics: List[Dict[str, Any]] = []

class MmsiLookupRequest(BaseModel):
    mmsis: List[str]

@app.post("/api/sync/batch")
async def receive_batch_sync(req: BatchSyncRequest):
    expected_token = os.environ.get("SYNC_TOKEN", SYNC_TOKEN)
    if req.token != expected_token:
        raise HTTPException(status_code=401, detail="Ungültiger Sync-Token")

    flush_batches(req.positions, req.statics)
    return {
        "status": "ok",
        "received_positions": len(req.positions),
        "received_statics": len(req.statics),
        "total_archived": stream_state["positions_archived"]
    }

@app.post("/api/vessels/lookup")
async def lookup_vessels(req: MmsiLookupRequest):
    if not req.mmsis:
        return {"vessels": [], "count": 0}
    clean_mmsis = [str(m).strip() for m in req.mmsis if str(m).strip()][:500]
    placeholders = ",".join("?" for _ in clean_mmsis)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM vessels WHERE mmsi IN ({placeholders})", clean_mmsis)
            rows = [dict(r) for r in cur.fetchall()]
            return {"vessels": rows, "count": len(rows)}
    except Exception as e:
        logger.error(f"Lookup error: {e}")
        return {"vessels": [], "count": 0, "error": str(e)}

@app.get("/api/export/mmsi.json")
async def export_mmsi_json():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM vessels ORDER BY mmsi ASC")
            rows = [dict(r) for r in cur.fetchall()]
            return JSONResponse(
                content=rows,
                headers={"Content-Disposition": f"attachment; filename=mmsi_database_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"}
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/bight.db")
async def export_db_file():
    if os.path.exists(DB_FILE):
        return FileResponse(
            DB_FILE,
            media_type="application/x-sqlite3",
            filename=f"bight_{datetime.now(timezone.utc).strftime('%Y%m%d')}.db"
        )
    raise HTTPException(status_code=404, detail="Datenbank nicht gefunden")

def get_fleet_memberships(cur) -> tuple[List[Dict[str, Any]], Dict[str, List[str]], Dict[str, str]]:
    cur.execute("SELECT id, name, icon, color, is_system FROM fleets ORDER BY is_system DESC, name ASC")
    fleets = [dict(zip(["id", "name", "icon", "color", "is_system"], r)) for r in cur.fetchall()]

    cur.execute("SELECT fleet_id, mmsi, custom_name, notes FROM fleet_vessels")
    fv_rows = cur.fetchall()
    mmsi_to_fleets: Dict[str, List[str]] = {}
    mmsi_custom_name: Dict[str, str] = {}
    fleet_counts: Dict[str, int] = {f["id"]: 0 for f in fleets}

    for f_id, mmsi, c_name, notes in fv_rows:
        mmsi_to_fleets.setdefault(mmsi, []).append(f_id)
        if c_name and mmsi not in mmsi_custom_name:
            mmsi_custom_name[mmsi] = c_name
        if f_id in fleet_counts:
            fleet_counts[f_id] += 1

    for f in fleets:
        f["vessel_count"] = fleet_counts.get(f["id"], 0)

    return fleets, mmsi_to_fleets, mmsi_custom_name

@app.get("/api/fleets")
async def list_fleets():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            fleets, _, _ = get_fleet_memberships(cur)
            for f in fleets:
                cur.execute("""
                    SELECT fv.mmsi, fv.custom_name, fv.notes,
                           v.name as ais_name, v.category, v.category_label,
                           v.latitude, v.longitude, v.speed, v.course, v.last_seen, v.destination
                    FROM fleet_vessels fv
                    LEFT JOIN vessels v ON v.mmsi = fv.mmsi
                    WHERE fv.fleet_id = ?
                    ORDER BY (v.last_seen IS NOT NULL) DESC, v.last_seen DESC, fv.custom_name ASC
                """, (f["id"],))
                f["vessels"] = [dict(r) for r in cur.fetchall()]
                f["active_count"] = sum(1 for x in f["vessels"] if x.get("latitude") is not None)
            return {"fleets": fleets}
    except Exception as e:
        logger.error(f"Error in /api/fleets: {e}")
        return {"fleets": []}

@app.post("/api/fleets")
async def create_fleet(req: FleetCreateRequest):
    slug = re.sub(r'[^a-z0-9_]+', '_', req.name.lower().strip()).strip('_') or f"fleet_{int(time.time())}"
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM fleets WHERE id = ?", (slug,))
            if cur.fetchone()[0] > 0:
                slug = f"{slug}_{int(time.time()) % 10000}"
            cur.execute(
                "INSERT INTO fleets (id, name, icon, color, is_system) VALUES (?, ?, ?, ?, 0)",
                (slug, req.name.strip(), req.icon or "⭐", req.color or "#f59e0b")
            )
            conn.commit()
            return {"status": "ok", "id": slug}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/fleets/{fleet_id}")
async def delete_fleet(fleet_id: str):
    if fleet_id in ("navy", "my_fleet"):
        raise HTTPException(status_code=400, detail="Standard-Flotten können nicht gelöscht werden.")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM fleet_vessels WHERE fleet_id = ?", (fleet_id,))
        conn.execute("DELETE FROM fleets WHERE id = ? AND is_system = 0", (fleet_id,))
        conn.commit()
    return {"status": "ok"}

@app.post("/api/fleets/{fleet_id}/vessels")
async def add_vessel_to_fleet(fleet_id: str, req: FleetVesselRequest):
    mmsi = req.mmsi.strip()
    if not mmsi:
        raise HTTPException(status_code=400, detail="MMSI fehlt")
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        c_name = (req.custom_name or "").strip() or None
        cur.execute(
            "INSERT OR REPLACE INTO fleet_vessels (fleet_id, mmsi, custom_name, notes) VALUES (?, ?, ?, ?)",
            (fleet_id, mmsi, c_name, req.notes)
        )
        if c_name:
            cur.execute(
                "UPDATE vessels SET name = ? WHERE mmsi = ? AND (name IS NULL OR name LIKE 'MMSI %')",
                (c_name, mmsi)
            )
        conn.commit()
    return {"status": "ok", "fleet_id": fleet_id, "mmsi": mmsi}

@app.delete("/api/fleets/{fleet_id}/vessels/{mmsi}")
async def remove_vessel_from_fleet(fleet_id: str, mmsi: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM fleet_vessels WHERE fleet_id = ? AND mmsi = ?", (fleet_id, mmsi.strip()))
        conn.commit()
    return {"status": "ok"}

@app.get("/api/vessels")
async def get_live_vessels(
    request: Request,
    max_age_minutes: int = Query(120, description="Maximales Alter für Live-Karte in Minuten (Standard 120 Min, bis 43200 = 30 Tage)"),
    category: Optional[str] = None,
    fleet_id: Optional[str] = None,
    search: Optional[str] = None
):
    """Gibt alle Schiffe zurück, die innerhalb von max_age_minutes eine Position gemeldet haben.
    Öffentlich auch ohne Login zugänglich, damit die Live-Karte sofort einsehbar ist."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    has_explicit_search = bool(search and len(search.strip()) >= 2)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            fleets, mmsi_to_fleets, mmsi_custom_name = get_fleet_memberships(cur)

            if has_explicit_search:
                query = """
                    SELECT * FROM vessels
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """
                params: List[Any] = []
            else:
                query = """
                    SELECT * FROM vessels
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                      AND last_seen >= ?
                """
                params = [cutoff]

            if fleet_id and fleet_id != "ALL":
                query += " AND mmsi IN (SELECT mmsi FROM fleet_vessels WHERE fleet_id = ?)"
                params.append(fleet_id)

            if category and category != "ALL":
                query += " AND category = ?"
                params.append(category)

            if search and search.strip():
                q = f"%{search.strip()}%"
                query += " AND (name LIKE ? OR mmsi LIKE ? OR callsign LIKE ? OR imo LIKE ? OR destination LIKE ?)"
                params.extend([q, q, q, q, q])

            query += " ORDER BY last_seen DESC LIMIT 3000"
            cur.execute(query, params)
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                m = d["mmsi"]
                d["fleets"] = mmsi_to_fleets.get(m, [])
                if (not d.get("name") or d["name"].startswith("MMSI ")) and m in mmsi_custom_name:
                    d["name"] = mmsi_custom_name[m]
                d["is_historical"] = bool(d.get("last_seen") and d["last_seen"] < cutoff)
                rows.append(d)

            cur.execute("SELECT COUNT(*) FROM vessels")
            total_mmsi_db = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM position_history")
            total_waypoints = cur.fetchone()[0]

            return {
                "vessels": rows,
                "fleets": fleets,
                "active_count": len(rows),
                "total_mmsi_db": total_mmsi_db,
                "total_waypoints_30d": total_waypoints,
                "stream_state": stream_state
            }
    except Exception as e:
        logger.error(f"Error in /api/vessels: {e}")
        return {"vessels": [], "fleets": [], "active_count": 0, "total_mmsi_db": 0, "total_waypoints_30d": 0, "stream_state": stream_state}

@app.get("/api/mmsi-database")
async def get_mmsi_database(
    request: Request,
    search: Optional[str] = None,
    category: Optional[str] = None,
    fleet_id: Optional[str] = None,
    country: Optional[str] = None,
    limit: int = 500,
    offset: int = 0
):
    """Durchsucht die gesamte aufgebaute MMSI-Schiffsdatenbank (auch Schiffe, die gerade außer Reichweite sind)."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            fleets, mmsi_to_fleets, mmsi_custom_name = get_fleet_memberships(cur)

            where_clauses = ["1=1"]
            params: List[Any] = []

            if fleet_id and fleet_id != "ALL":
                where_clauses.append("mmsi IN (SELECT mmsi FROM fleet_vessels WHERE fleet_id = ?)")
                params.append(fleet_id)

            if category and category != "ALL":
                where_clauses.append("category = ?")
                params.append(category)

            if country and country != "ALL":
                where_clauses.append("country = ?")
                params.append(country)

            if search:
                q = f"%{search.strip()}%"
                where_clauses.append("(name LIKE ? OR mmsi LIKE ? OR callsign LIKE ? OR imo LIKE ? OR destination LIKE ?)")
                params.extend([q, q, q, q, q])

            where_sql = " AND ".join(where_clauses)

            cur.execute(f"SELECT COUNT(*) FROM vessels WHERE {where_sql}", params)
            total_matches = cur.fetchone()[0]

            cur.execute(
                f"SELECT * FROM vessels WHERE {where_sql} ORDER BY last_seen DESC LIMIT ? OFFSET ?",
                params + [limit, offset]
            )
            items = []
            for r in cur.fetchall():
                d = dict(r)
                m = d["mmsi"]
                d["fleets"] = mmsi_to_fleets.get(m, [])
                if (not d.get("name") or d["name"].startswith("MMSI ")) and m in mmsi_custom_name:
                    d["name"] = mmsi_custom_name[m]
                items.append(d)

            return {
                "items": items,
                "fleets": fleets,
                "total": total_matches,
                "limit": limit,
                "offset": offset
            }
    except Exception as e:
        logger.error(f"Error in /api/mmsi-database: {e}")
        return {"items": [], "fleets": [], "total": 0}

@app.get("/api/history/{mmsi}")
async def get_vessel_history(
    mmsi: str,
    request: Request,
    hours: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 5000
):
    """Gibt die gespeicherte 30-Tage-Route eines Schiffes zurück."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            query = "SELECT latitude, longitude, speed, course, heading, status, timestamp FROM position_history WHERE mmsi = ?"
            params: List[Any] = [mmsi]

            if hours:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
                query += " AND timestamp >= ?"
                params.append(cutoff)
            else:
                if start:
                    s_clean = normalize_iso_timestamp(start)
                    query += " AND timestamp >= ?"
                    params.append(s_clean)
                if end:
                    e_clean = normalize_iso_timestamp(end)
                    query += " AND timestamp <= ?"
                    params.append(e_clean)

            query += " ORDER BY timestamp ASC LIMIT ?"
            params.append(limit)

            cur.execute(query, params)
            points = [dict(r) for r in cur.fetchall()]

            cur.execute("SELECT * FROM vessels WHERE mmsi = ?", (mmsi,))
            v_row = cur.fetchone()

            return {
                "mmsi": mmsi,
                "vessel": dict(v_row) if v_row else None,
                "total_points": len(points),
                "points": points
            }
    except Exception as e:
        logger.error(f"Error in /api/history/{mmsi}: {e}")
        return {"mmsi": mmsi, "points": [], "total_points": 0}

@app.get("/api/stats")
async def get_stats():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT category, category_label, COUNT(*) FROM vessels GROUP BY category ORDER BY COUNT(*) DESC")
            by_cat = [{"category": r[0], "label": r[1], "count": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT country, COUNT(*) FROM vessels GROUP BY country ORDER BY COUNT(*) DESC LIMIT 15")
            by_country = [{"country": r[0], "count": r[1]} for r in cur.fetchall()]

            cur.execute("SELECT COUNT(*) FROM vessels")
            total_vessels = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM position_history")
            total_points = cur.fetchone()[0]

            cfg = load_config()
            return {
                "total_vessels": total_vessels,
                "total_points": total_points,
                "by_category": by_cat,
                "by_country": by_country,
                "stream_state": stream_state,
                "config": {
                    "retention_days": cfg.get("retention_days", 30),
                    "bbox": cfg.get("bbox", DEFAULT_BBOX),
                    "aisstream_key": cfg.get("aisstream_key", AISSTREAM_KEY),
                    "datadocked_key": cfg.get("datadocked_key", ""),
                    "carto_key": cfg.get("carto_key", CARTO_KEY),
                    "allow_registration": cfg.get("allow_registration", True)
                }
            }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/settings")
async def update_settings(req: SettingsUpdate, request: Request):
    require_admin(request)
    cfg = load_config()
    cfg["retention_days"] = max(1, min(90, req.retention_days))
    if req.allow_registration is not None:
        cfg["allow_registration"] = bool(req.allow_registration)
    if req.aisstream_key is not None and req.aisstream_key.strip():
        if req.aisstream_key.strip() != cfg.get("aisstream_key"):
            cfg["aisstream_key"] = req.aisstream_key.strip()
            stream_state["reconnect_requested"] = True
    if req.datadocked_key is not None:
        cfg["datadocked_key"] = req.datadocked_key.strip()
    if req.carto_key is not None and req.carto_key.strip():
        cfg["carto_key"] = req.carto_key.strip()

    if req.lat_min is not None and req.lon_min is not None and req.lat_max is not None and req.lon_max is not None:
        lat1, lat2 = sorted([round(float(req.lat_min), 4), round(float(req.lat_max), 4)])
        lon1, lon2 = sorted([round(float(req.lon_min), 4), round(float(req.lon_max), 4)])
        new_bbox = [[[lat1, lon1], [lat2, lon2]]]
        if new_bbox != cfg.get("bbox"):
            cfg["bbox"] = new_bbox
            stream_state["bbox"] = new_bbox
            stream_state["reconnect_requested"] = True
    elif req.bbox:
        cfg["bbox"] = req.bbox
        stream_state["bbox"] = req.bbox
        stream_state["reconnect_requested"] = True

    save_config(cfg)
    stream_state["retention_days"] = cfg["retention_days"]
    return {
        "status": "ok",
        "retention_days": cfg["retention_days"],
        "bbox": cfg.get("bbox", DEFAULT_BBOX)
    }

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(aisstream_worker())
    asyncio.create_task(retention_cleanup_worker())
    if CLOUD_SYNC_ENABLED:
        asyncio.create_task(cloud_sync_worker())

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def root_index():
    return FileResponse(os.path.join(static_dir, "index.html"))
