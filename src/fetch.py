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

UK_REGIONS = [
    ("London", 51.5074, -0.1278),
    ("Manchester", 53.4808, -2.2426),
    ("Birmingham", 52.4862, -1.8904),
    ("Leeds", 53.8008, -1.5491),
    ("Bristol", 51.4545, -2.5879),
    ("Brighton", 50.8225, -0.1372),
    ("Oxford", 51.7520, -1.2577),
    ("Cambridge", 52.2053, 0.1218),
    ("Milton Keynes", 52.0406, -0.7594),
    ("Reading", 51.4543, -0.9781),
]

def init_db():
    """Erstellt die SQLite-Datenbank und Tabellen falls nicht vorhanden."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # --- Tabelle stations ---
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

    # --- Tabelle status_history ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            status TEXT,
            comment_type_title TEXT,
            checkin_status_title TEXT,
            comment_text TEXT,
            is_operational BOOLEAN,
            timestamp TEXT,
            raw_json TEXT
        )
    """)

    # --- Neue Tabelle comments_history ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS comments_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER,
            comment_ocm_id INTEGER,
            comment_type TEXT,
            checkin_status TEXT,
            comment_text TEXT,
            comment_date TEXT,
            raw_json TEXT,
            UNIQUE(station_id, comment_ocm_id)
        )
    """)

    # --- TEMP: Region Activity (Testphase) ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS region_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT,
            country_code TEXT,
            latitude REAL,
            longitude REAL,
            stations_count INTEGER,
            stations_with_comments INTEGER,
            total_comments INTEGER,
            run_timestamp TEXT
        )
    """)

    conn.commit()
    conn.close()

def scan_uk_regions(radius_km=15, max_results=300):
    print("🇬🇧 Starte UK Region Scan (OCM)...\n")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for name, lat, lon in UK_REGIONS:
        print(f"🔍 Region: {name}")

        url = (
            "https://api.openchargemap.io/v3/poi/"
            "?output=json"
            "&countrycode=GB"
            f"&latitude={lat}"
            f"&longitude={lon}"
            f"&distance={radius_km}"
            "&distanceunit=KM"
            f"&maxresults={max_results}"
            f"&key={API_KEY}"
        )

        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"❌ Fehler bei {name}: {e}")
            continue

        stations_count = len(data)
        stations_with_comments = 0
        total_comments = 0

        for d in data:
            comments = d.get("UserComments") or []
            if comments:
                stations_with_comments += 1
                total_comments += len(comments)

        ts = datetime.datetime.now(timezone.utc).isoformat()

        c.execute("""
            INSERT INTO region_activity (
                region_name,
                country_code,
                latitude,
                longitude,
                stations_count,
                stations_with_comments,
                total_comments,
                run_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            "GB",
            lat,
            lon,
            stations_count,
            stations_with_comments,
            total_comments,
            ts
        ))

        print(
            f"   ➤ Stationen: {stations_count} | "
            f"mit Kommentaren: {stations_with_comments} | "
            f"Kommentare gesamt: {total_comments}"
        )

        time.sleep(2)  # API-freundlich

    conn.commit()
    conn.close()

    print("\n✅ UK Region Scan abgeschlossen.\n")


def fetch_from_api(max_results=200):
    """Ruft kommentierte Ladepunkte im Berliner Raum ab und zeigt Analyse über Kommentaraktivität."""

    # Berlin Zentrum
    latitude = 52.5200
    longitude = 13.4050
    radius_km = 20

    url = (
        "https://api.openchargemap.io/v3/poi/"
        "?output=json"
        "&countrycode=DE"
        f"&latitude={latitude}"
        f"&longitude={longitude}"
        f"&distance={radius_km}"
        "&distanceunit=KM"
        "&mincomments=1"
        f"&maxresults={max_results}"
        f"&key={API_KEY}"
    )

    print(f"\n🌐 API URL:\n{url}\n")

    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        n = len(data)
        print(f"✅ API Response OK — {n} Stationen mit mindestens einem Kommentar erhalten.\n")

        # ------------------------
        # COMMENT ANALYTICS
        # ------------------------

        comment_counts = []
        station_summaries = []

        for d in data:
            sid = d.get("ID")
            comments = d.get("UserComments") or []
            cnt = len(comments)
            comment_counts.append(cnt)
            station_summaries.append((sid, cnt))

        total_comments = sum(comment_counts)
        max_comments = max(comment_counts) if comment_counts else 0
        avg_comments = total_comments / n if n > 0 else 0

        # Sortieren nach Anzahl Kommentare
        station_summaries.sort(key=lambda x: x[1], reverse=True)

        print("📊 Kommentar-Analyse:")
        print(f"   ➤ Anzahl Stationen: {n}")
        print(f"   ➤ Gesamtanzahl Kommentare: {total_comments}")
        print(f"   ➤ Ø Kommentare pro Station: {avg_comments:.2f}")
        print(f"   ➤ Max Kommentare an einer Station: {max_comments}")
        print()

        # Top 10 Stationen
        print("🏆 Top kommentierte Stationen (Top 10):")
        for sid, cnt in station_summaries[:10]:
            print(f"   - Station {sid}: {cnt} Kommentare")

        # Optionales Histogramm
        print("\n📈 Histogramm der Kommentarhäufigkeit:")
        hist = {}
        for cnt in comment_counts:
            hist[cnt] = hist.get(cnt, 0) + 1
        for cnt in sorted(hist):
            print(f"   {cnt} Kommentare: {hist[cnt]} Stationen")

        print("\n")

        return data

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

            # === Kommentare separat verarbeiten (NEUE Event-Tabelle) ===

            comments = d.get("UserComments") or []

            for comment in comments:
                try:
                    comment_ocm_id = comment.get("ID")  # OCM interne Kommentar-ID
                    if comment_ocm_id is None:
                        continue  # Kommentar kann nur mit ID gespeichert werden

                    comment_type = safe_get(comment, "CommentType", "Title")
                    checkin_status = safe_get(comment, "CheckinStatusType", "Title")
                    comment_text = comment.get("Comment")
                    comment_date = comment.get("DateCreated") or comment.get("DateLastModified")

                    # Prüfe, ob dieser Kommentar schon existiert
                    c.execute("""
                        SELECT 1 FROM comments_history
                        WHERE station_id = ? AND comment_ocm_id = ?
                    """, (station_id, comment_ocm_id))

                    exists = c.fetchone()

                    if not exists:
                        # Neuer Kommentar → speichern
                        c.execute("""
                            INSERT INTO comments_history (
                                station_id,
                                comment_ocm_id,
                                comment_type,
                                checkin_status,
                                comment_text,
                                comment_date,
                                raw_json
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            station_id,
                            comment_ocm_id,
                            comment_type,
                            checkin_status,
                            comment_text,
                            comment_date,
                            json.dumps(comment)
                        ))

                        print(f"💬 Neuer Kommentar gespeichert: Station {station_id}, CommentID={comment_ocm_id}")

                except Exception as e:
                    print(f"⚠️ Fehler beim Verarbeiten eines Kommentars an Station {station_id}: {e}")

            # Alter Code-Block: Noch behalten, bis MVP fertig ist
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

def run_region_scan():
    init_db()
    scan_uk_regions()


if __name__ == "__main__":
    # init_db()
    # run()
    run_region_scan()