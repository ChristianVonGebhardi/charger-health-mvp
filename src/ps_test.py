import requests
import json
from datetime import datetime

# ----------------------------------------
# PlugShare Region Fetch
# ----------------------------------------

def fetch_plugshare_region(lat, lon, dist_km=20):
    url = f"https://www.plugshare.com/api/locations/region?lat={lat}&lng={lon}&distance={dist_km}"
    print(f"🌐 Fetching PlugShare Region:\n{url}\n")
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        print(f"✅ {len(data)} Stationen in der Region gefunden.\n")
        return data
    except Exception as e:
        print("❌ Error:", e)
        return None


def fetch_plugshare_details(location_id):
    url = f"https://www.plugshare.com/api/locations/{location_id}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def run_test():
    lat = 52.5200
    lon = 13.4050
    dist_km = 20

    region = fetch_plugshare_region(lat, lon, dist_km)
    if not region:
        print("❌ Keine Daten.")
        return

    ids = [item.get("id") for item in region if item.get("id")]
    print(f"📌 Station IDs (Erste 20): {ids[:20]}\n")

    comment_counts = []
    recent_comments = []
    failed_charges = 0

    print("🔍 Hole Details für die ersten 50 Stationen...\n")

    for loc_id in ids[:50]:
        details = fetch_plugshare_details(loc_id)
        if not details:
            continue

        comments = (details.get("address") or {}).get("comments") or []
        comment_counts.append(len(comments))

        for c in comments:
            ts = c.get("ts")
            text = c.get("comment")
            status = c.get("status")
            if ts:
                recent_comments.append((loc_id, ts, status, text))
                if status and "fail" in status.lower():
                    failed_charges += 1

    total = len(comment_counts)
    avg = sum(comment_counts) / total if total else 0
    maxi = max(comment_counts) if total else 0

    print("\n📊 Kommentar-Analyse:")
    print(f"   ➤ Anzahl getesteter Stationen: {total}")
    print(f"   ➤ Gesamt-Kommentare: {sum(comment_counts)}")
    print(f"   ➤ Ø Kommentare pro Station: {avg:.2f}")
    print(f"   ➤ Max Kommentare an einer Station: {maxi}")
    print(f"   ➤ Failed-Charge-Reports: {failed_charges}")

    recent_comments.sort(key=lambda x: x[1], reverse=True)

    print("\n🕒 Neueste Kommentare (Top 10):")
    for loc_id, ts, status, text in recent_comments[:10]:
        t = datetime.fromtimestamp(ts/1000).isoformat()
        print(f"   • Station {loc_id} | {t} | {status} | {text}")


if __name__ == "__main__":
    run_test()