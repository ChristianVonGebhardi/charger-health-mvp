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


def get_last_status(c, station_id):
    c.execute("""
        SELECT status FROM status_history
        WHERE station_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (station_id,))
    row = c.fetchone()
    return row[0] if row else None


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

            # Dynamische Statusdaten
            status = safe_get(d, "StatusType", "Title")
            is_operational = safe_get(d, "StatusType", "IsOperational")

            # === User Comments & Check-In Feedback aus OCM extrahieren ===
            comment_type_title = None
            checkin_status_title = None
            comment_text = None

            comments = d.get("UserComments") or []

            for comment in comments:
                # CommentType (z. B. "General Comment", "Problem Report")
                ct = comment.get("CommentType", {})
                if not comment_type_title and ct:
                    comment_type_title = ct.get("Title")

                # CheckinStatusType (z. B. "Successfully Charged", "Charging Not Possible")
                cs = comment.get("CheckinStatusType", {})
                if not checkin_status_title and cs:
                    checkin_status_title = cs.get("Title")

                # Freitext des Nutzers
                if not comment_text:
                    comment_text = comment.get("Comment")

                # Wenn alle Felder gefunden → abbrechen
                if comment_type_title and checkin_status_title and comment_text:
                    break

            # Debug-Ausgabe
            if comment_type_title or checkin_status_title:
                print(f"✅ Station {station_id} | Checkin='{checkin_status_title}' | Type='{comment_type_title}' | Text='{comment_text}'")

            # Letzten Status laden
            c.execute("""
                SELECT status, comment_type_title, checkin_status_title, comment_text
                FROM status_history
                WHERE station_id = ?
                ORDER BY id DESC
                LIMIT 1
            """, (station_id,))
            last_row = c.fetchone()

            last_status = last_row[0] if last_row else None
            last_comment_type = last_row[1] if last_row else None
            last_checkin_status = last_row[2] if last_row else None
            last_comment_text = last_row[3] if last_row else None

            # Prüfen, ob eine neue Zeile notwendig ist
            changed = (
                last_status != status or
                last_comment_type != comment_type_title or
                last_checkin_status != checkin_status_title or
                last_comment_text != comment_text
            )

            if changed:
                timestamp = datetime.datetime.now(timezone.utc).isoformat()
                c.execute("""
                    INSERT INTO status_history (
                        station_id,
                        status,
                        is_operational,
                        timestamp,
                        raw_json,
                        comment_type_title,
                        checkin_status_title,
                        comment_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    station_id,
                    status,
                    is_operational,
                    timestamp,
                    json.dumps(d),
                    comment_type_title,
                    checkin_status_title,
                    comment_text
                ))

        except Exception as e:
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