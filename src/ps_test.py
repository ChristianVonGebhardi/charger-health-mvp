import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime

# ----------------------------------------
# Authentifizierungsdaten laden (aus .env)
# ----------------------------------------

load_dotenv()

AUTH_BASIC = os.getenv("PLUGSHARE_AUTH_BASIC")
#COGNITO = os.getenv("PLUGSHARE_COGNITO")

token = os.getenv("PLUGSHARE_TOKEN")
if token is None:
    print("PLUGSHARE_TOKEN ist nicht gesetzt!")
else:
    print("Using token:", token[:20] + "...")

# URL, die du im Browser siehst
url = "https://api.plugshare.com/v3/locations/region?access=1&count=100&latitude=52.52&longitude=13.405&spanLat=0.1&spanLng=0.2&minimal=0"

# Token und Cognito aus .env
token = os.getenv("PLUGSHARE_AUTH_BASIC")
cognito = os.getenv("PLUGSHARE_TOKEN")

# Headers aus dem Browser kopiert und angepasst
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Authorization": f"Bearer {token}",
    "Cognito-Token": cognito,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    # Optional: Cookie aus Browser einfügen, falls PlugShare es verlangt
    # "Cookie": "<hier deine Browser-Cookies>"
}

response = requests.get(url, headers=headers)

print("Status Code:", response.status_code)
if response.status_code == 200:
    data = response.json()
    print("Anzahl Locations:", len(data.get("locations", [])))
else:
    print("Fehler:", response.text)

headers = {"Authorization": f"Bearer {token}"}
#response = requests.get("https://api.plugshare.com/some/endpoint", headers=headers)
response = requests.get("https://api.plugshare.com/v3/locations/region?access=1", headers=headers)

print(response.status_code)
print(response.text)

print("AUTH_BASIC:", AUTH_BASIC[:8] + "...")
#print("COGNITO:", COGNITO[:20] + "...")


HEADERS = {
    "accept": "application/json, text/plain, */*",
    "authorization": f"Basic {AUTH_BASIC}",
    "cognito-authorization": token,
    "origin": "https://www.plugshare.com",
    "referer": "https://www.plugshare.com/",
    "user-agent": "Mozilla/5.0",
}

BASE_URL = "https://api.plugshare.com/v3"


# ----------------------------------------
# PlugShare Region Fetch
# ----------------------------------------

def fetch_plugshare_region(lat, lon, span_lat=0.1, span_lng=0.2):
    url = f"{BASE_URL}/locations/region"
    params = {
        "access": 1,
        "count": 100,
        "latitude": lat,
        "longitude": lon,
        "spanLat": span_lat,
        "spanLng": span_lng,
        "minimal": 0,
    }
    url =f"https://api.plugshare.com/v3/locations/region?access=1&count=500&latitude=51.333521601002694&longitude=-116.99865691184972&minimal=0&outlets=%5B%7B%22connector%22:13,%22power%22:0%7D,%7B%22connector%22:2,%22power%22:0%7D%5D&spanLat=0.2037781312950102&spanLng=0.276031494140625"
    print(f"🌐 Fetching PlugShare Region:\n{url}\n")
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        print(f"✅ {len(data)} Stationen in der Region gefunden.\n")
        return data
    except Exception as e:
        print("❌ Error:", e)
        return None


def fetch_plugshare_details(location_id):
    url = f"{BASE_URL}/locations/{location_id}"
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def run_test():
    lat, lon = 52.5200, 13.4050  # Berlin
    #lat, lon = 51.2947, -117.0203
    region = fetch_plugshare_region(lat, lon)
    if not region:
        print("❌ Keine Daten.")
        return

    ids = [item.get("id") for item in region if item.get("id")]
    print(f"📌 Station IDs (Erste 20): {ids[:20]}\n")

    comment_counts, recent_comments = [], []
    failed_charges = 0

    print("🔍 Hole Details für die ersten 50 Stationen...\n")

    for loc_id in ids[:50]:
        details = fetch_plugshare_details(loc_id)
        if not details:
            continue

        comments = details.get("comments", [])
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