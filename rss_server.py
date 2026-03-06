from flask import Flask, Response
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
from urllib.parse import quote
from cachetools import cached, TTLCache
import logging
import pytz

# Konfiguriere Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

STOP_ID = "8006336"
STOP_NAME = "Wennigsen (Deister) Bahnhof"
API_URL = f"https://v6.db.transport.rest/stops/{STOP_ID}/departures"
TRIPS_URL = "https://v6.db.transport.rest/trips"

# Cache-Konfiguration (2 Minuten TTL)
cache = TTLCache(maxsize=100, ttl=120)

HEADERS = {
    "User-Agent": "Wennigsen-RSS-Feed-Bot/1.1 (https://wennigsen-rss-feed.onrender.com)"
}

@cached(cache)
def fetch_departures(results=15, duration=120):
    params = {"results": results, "duration": duration, "language": "de", "pretty": "false"}
    try:
        # SSL-Verifizierung deaktiviert, da die API oft Zertifikatsprobleme hat
        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=10, verify=False)
        response.raise_for_status()
        data = response.json()
        deps = data.get("departures", [])
        logging.info(f"Successfully fetched {len(deps)} departures.")
        return deps
    except Exception as e:
        logging.error(f"Error fetching departures: {e}")
        return []

@cached(cache)
def fetch_stopovers(trip_id, current_stop_id):
    try:
        encoded_trip_id = quote(trip_id, safe='')
        url = f"{TRIPS_URL}/{encoded_trip_id}?stopovers=true"
        response = requests.get(url, headers=HEADERS, timeout=10, verify=False)
        response.raise_for_status()
        data = response.json()
        stopovers = data.get("trip", {}).get("stopovers", [])
        
        found_current = False
        following_stops = []
        for stop in stopovers:
            stop_id = stop.get("stop", {}).get("id", "")
            if stop_id == current_stop_id or stop_id == STOP_ID:
                found_current = True
                continue
            if found_current:
                following_stops.append(stop)
        return following_stops
    except Exception as e:
        logging.error(f"Error fetching stopovers for trip {trip_id}: {e}")
        return []

def parse_iso_time(iso_time):
    """Parst ISO-Zeitstempel und gibt ein timezone-aware datetime-Objekt in Europe/Berlin zurück."""
    if not iso_time:
        return None
    try:
        # Ersetzt 'Z' durch +00:00 für die Kompatibilität mit fromisoformat
        fixed_iso = iso_time.replace("Z", "+00:00")
        dt_obj = datetime.fromisoformat(fixed_iso)
        
        # Wenn das Objekt timezone-naiv ist, gehe davon aus, dass es UTC ist und konvertiere
        if dt_obj.tzinfo is None:
            dt_obj = pytz.utc.localize(dt_obj)
        
        # Konvertiere in die lokale Zeitzone (Europe/Berlin)
        berlin_tz = pytz.timezone("Europe/Berlin")
        return dt_obj.astimezone(berlin_tz)
    except ValueError:
        logging.warning(f"Could not parse ISO time: {iso_time}")
        return None

def format_time(dt):
    """Formatiert ein datetime-Objekt zu HH:MM."""
    if not dt:
        return "---"
    return dt.strftime("%H:%M")

def format_delay(delay):
    if delay is None or delay == 0:
        return ""
    minutes = delay // 60
    if minutes > 0:
        return f" (+{minutes})"
    return ""

def format_stopovers(stopovers):
    if not stopovers:
        return "Keine weiteren Halte verfügbar"
    lines = []
    for stop in stopovers:
        stop_name = stop.get("stop", {}).get("name", "---")
        arrival_dt = parse_iso_time(stop.get("arrival"))
        arrival_time = format_time(arrival_dt)
        delay = stop.get("arrivalDelay", 0)
        delay_str = ""
        if delay and delay > 0:
            delay_min = delay // 60
            delay_str = f" (+{delay_min})"
        lines.append(f"{arrival_time}{delay_str} {stop_name}")
    return "\n".join(lines)

def generate_rss_feed():
    departures = fetch_departures(results=15, duration=120)
    
    # Sortiere Abfahrten nach Zeit (wichtig, falls die API sie unsortiert liefert)
    # Wir nutzen 'when' oder 'plannedWhen' als Fallback
    departures.sort(key=lambda x: x.get('when') or x.get('plannedWhen') or "")

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = f"Abfahrten {STOP_NAME}"
    ET.SubElement(channel, "link").text = "https://www.gvh.de"
    ET.SubElement(channel, "description").text = f"Nächste Abfahrten am {STOP_NAME}"
    ET.SubElement(channel, "language").text = "de-de"

    if not departures:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = "Keine Abfahrten verfügbar"
    else:
        for dep in departures:
            item = ET.SubElement(channel, "item")
            line_name = dep.get("line", {}).get("name", "---")
            direction = dep.get("direction", "---")
            
            # Zeitverarbeitung
            when_dt = parse_iso_time(dep.get("when"))
            when_str = format_time(when_dt)
            
            delay = dep.get("delay", 0)
            delay_str = format_delay(delay)
            platform = dep.get("platform") or dep.get("plannedPlatform") or ""
            platform_str = f" Gl.{platform}" if platform else ""
            
            # Titel des RSS-Items
            ET.SubElement(item, "title").text = f"{when_str}{delay_str} {line_name} -> {direction}{platform_str}"
            
            # Beschreibung (Zwischenhalte)
            item_desc = ET.SubElement(item, "description")
            trip_id = dep.get("tripId", "")
            stop_id = dep.get("stop", {}).get("id", STOP_ID)
            if trip_id:
                stopovers = fetch_stopovers(trip_id, stop_id)
                item_desc.text = format_stopovers(stopovers)
            else:
                item_desc.text = f"Linie: {line_name} | Richtung: {direction}"
    
    # XML Pretty Print
    rough_string = ET.tostring(rss, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

@app.route("/")
def index():
    return "<h1>RSS-Feed Wennigsen Bahnhof</h1><p><a href='/feed.rss'>Zum RSS-Feed</a></p>"

@app.route("/feed.rss")
@app.route("/feed")
def rss_feed():
    feed = generate_rss_feed()
    return Response(feed, mimetype="application/rss+xml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
