# src/fetch.py
import requests
import sqlite3
import datetime
import json
import os
import time
from datetime import timezone

API_KEY = os.environ.get("OCM_API_KEY")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # src/
DB_PATH = os.path.join(BASE_DIR, "..", "data", "ev.db")
MAX_RESULTS = 500
REQUEST_TIMEOUT = 15  # Sekunden


def init_db():
    """Erstellt die SQLite-Datenbank und Tabellen falls nicht vorhanden."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id INTEGER PRIMARY KEY,
            title TEXT,
            operator TEXT,
            lat REAL,
            lon REAL,
            max_power_kw REAL,
            num_points INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            status TEXT,
            is_operational BOOLEAN,
            timestamp TEXT,
            raw_json TEXT
        )
    """)

    conn.commit()
    conn.close()


def fetch_from_api(max_results=200):
    """Ruft Daten von OpenChargeMap ab (API-Key im Query-String)."""
    url = f"https://api.openchargemap.io/v3/poi/?output=json&countrycode=DE&maxresults={max_results}&key={API_KEY}"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print("❌ API request failed:", e)
        return None


def safe_get(dct, *keys):
    """Hilfsfunktion: verschachtelte dict-get mit None-Safe."""
    cur = dct
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


def save_to_db(data):
    """Speichert API-Daten in SQLite."""
    if not data:
        print("⚠️ Keine Daten zu speichern.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for d in data:
        try:
            station_id = d.get("ID")
            if station_id is None:
                continue

            # Statische Daten (einmalig oder upsert)
            title = safe_get(d, "AddressInfo", "Title")
            operator = safe_get(d, "OperatorInfo", "Title")
            lat = safe_get(d, "AddressInfo", "Latitude")
            lon = safe_get(d, "AddressInfo", "Longitude")

            # Ladeleistungen extrahieren
            connections = d.get("Connections") or []
            max_power = None
            if connections:
                power_vals = []
                for fconn in connections:
                    # manche Einträge haben "PowerKW" oder "Level" oder None
                    pw = fconn.get("PowerKW") if isinstance(fconn, dict) else None
                    if pw:
                        power_vals.append(pw)
                if power_vals:
                    max_power = max(power_vals)

            num_points = d.get("NumberOfPoints", None)

            # Upsert für stations (SQLite >= 3.24 für ON CONFLICT DO UPDATE)
            c.execute("""
                INSERT INTO stations (station_id, title, operator, lat, lon, max_power_kw, num_points)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(station_id) DO UPDATE SET
                    title=excluded.title,
                    operator=excluded.operator,
                    lat=excluded.lat,
                    lon=excluded.lon,
                    max_power_kw=excluded.max_power_kw,
                    num_points=excluded.num_points
            """, (station_id, title, operator, lat, lon, max_power, num_points))

            # Dynamische Statusdaten (defensiv)
            status = safe_get(d, "StatusType", "Title")
            is_operational = safe_get(d, "StatusType", "IsOperational")

            # Zeitstempel als timezone-aware ISO string
            timestamp = datetime.datetime.now(timezone.utc).isoformat()

            c.execute("""
                INSERT INTO status_history (station_id, status, is_operational, timestamp, raw_json)
                VALUES (?, ?, ?, ?, ?)
            """, (station_id, status, is_operational, timestamp, json.dumps(d)))

        except Exception as e:
            # Fehler pro Eintrag loggen, aber Schleife fortsetzen
            print(f"⚠️ Fehler beim Verarbeiten station {d.get('ID')}: {e}")

    conn.commit()
    conn.close()


def run():
    print("🔄 Lade Daten von OpenChargeMap...")
    data = fetch_from_api(max_results=MAX_RESULTS)
    if data is None:
        print("❌ Keine Daten empfangen. Breche ab.")
        return

    print(f"✅ {len(data)} Ladepunkte erhalten. Speichere in Datenbank...")
    save_to_db(data)
    print("✅ Fertig! Status-Historie aktualisiert.")


if __name__ == "__main__":
    init_db()
    run()