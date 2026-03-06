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

# Konfiguration
STOP_ID_DB = "8006336"
STOP_ID_UESTRA = "25005782"
STOP_NAME = "Wennigsen (Deister) Bahnhof"

# API Endpunkte
API_DB = f"https://v6.db.transport.rest/stops/{STOP_ID_DB}/departures"
API_VBN = f"https://v6.vbn.transport.rest/stops/{STOP_ID_DB}/departures"
API_UESTRA = f"https://abfahrten.uestra.de/proxy2/efa/XML_DM_REQUEST?outputFormat=rapidJSON&depSequence=30&type_dm=any&name_dm={STOP_ID_UESTRA}&useRealtime=1"

TRIPS_URL_DB = "https://v6.db.transport.rest/trips"
TRIPS_URL_VBN = "https://v6.vbn.transport.rest/trips"

# Cache-Konfiguration (2 Minuten TTL)
cache = TTLCache(maxsize=100, ttl=120)

HEADERS = {
    "User-Agent": "Wennigsen-RSS-Feed-Bot/1.3 (https://wennigsen-rss-feed.onrender.com)"
}

def parse_iso_time(iso_time):
    """Parst ISO-Zeitstempel und gibt ein timezone-aware datetime-Objekt in Europe/Berlin zurück."""
    if not iso_time:
        return None
    try:
        # Behandelt Formate wie 2026-03-06T09:05:00.000Z oder 2026-03-06T09:05:00+01:00
        fixed_iso = iso_time.replace("Z", "+00:00")
        dt_obj = datetime.fromisoformat(fixed_iso)
        if dt_obj.tzinfo is None:
            dt_obj = pytz.utc.localize(dt_obj)
        berlin_tz = pytz.timezone("Europe/Berlin")
        return dt_obj.astimezone(berlin_tz)
    except Exception:
        return None

def format_time(dt):
    return dt.strftime("%H:%M") if dt else "---"

def process_uestra_data(data):
    """Konvertiert ÜSTRA-RapidJSON Format in das Standard-Format des Feeds."""
    departures = []
    raw_deps = data.get("departures", [])
    for rd in raw_deps:
        line_name = rd.get("line", "---")
        direction = rd.get("destination", "---")
        # ÜSTRA liefert mehrere Events pro Linie
        for event in rd.get("events", []):
            planned = event.get("plannedTime")
            actual = event.get("estimated_time") or planned
            
            planned_dt = parse_iso_time(planned)
            actual_dt = parse_iso_time(actual)
            
            delay_sec = 0
            if planned_dt and actual_dt:
                delay_sec = int((actual_dt - planned_dt).total_seconds())
            
            departures.append({
                "line": {"name": line_name},
                "direction": direction,
                "when": actual,
                "plannedWhen": planned,
                "delay": delay_sec,
                "platform": "", # ÜSTRA API liefert hier oft keine Gleise im RapidJSON
                "tripId": None # ÜSTRA Proxy unterstützt keine Trip-Details einfach
            })
    return departures

@cached(cache)
def fetch_departures():
    """Versucht Abfahrten von DB, VBN oder ÜSTRA zu laden."""
    # 1. Versuch: ÜSTRA (Oft am aktuellsten für Region Hannover)
    try:
        logging.info("Versuche Daten von ÜSTRA-API zu laden...")
        resp = requests.get(API_UESTRA, headers=HEADERS, timeout=5, verify=False)
        if resp.status_code == 200:
            deps = process_uestra_data(resp.json())
            if deps:
                logging.info(f"Erfolgreich {len(deps)} Abfahrten von ÜSTRA geladen.")
                return deps, None
    except Exception as e:
        logging.error(f"Fehler bei ÜSTRA: {e}")

    # 2. & 3. Versuch: DB und VBN
    apis = [("DB-API", API_DB, TRIPS_URL_DB), ("VBN-API", API_VBN, TRIPS_URL_VBN)]
    for name, url, trip_base_url in apis:
        try:
            logging.info(f"Versuche Daten von {name} zu laden...")
            response = requests.get(url, params={"results": 15, "duration": 120}, headers=HEADERS, timeout=8, verify=False)
            if response.status_code == 200:
                deps = response.json().get("departures", [])
                if deps:
                    logging.info(f"Erfolgreich {len(deps)} Abfahrten von {name} geladen.")
                    return deps, trip_base_url
        except Exception as e:
            logging.error(f"Fehler bei {name}: {e}")
            
    return [], None

@cached(cache)
def fetch_stopovers(trip_id, trip_base_url):
    if not trip_id or not trip_base_url:
        return []
    try:
        encoded_trip_id = quote(trip_id, safe='')
        url = f"{trip_base_url}/{encoded_trip_id}?stopovers=true"
        response = requests.get(url, headers=HEADERS, timeout=5, verify=False)
        if response.status_code == 200:
            stopovers = response.json().get("trip", {}).get("stopovers", [])
            found_current = False
            following_stops = []
            for stop in stopovers:
                s_id = stop.get("stop", {}).get("id", "")
                if s_id == STOP_ID_DB:
                    found_current = True
                    continue
                if found_current:
                    following_stops.append(stop)
            return following_stops
    except Exception:
        pass
    return []

def generate_rss_feed():
    departures, trip_base_url = fetch_departures()
    departures.sort(key=lambda x: x.get('when') or x.get('plannedWhen') or "")

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = f"Abfahrten {STOP_NAME}"
    ET.SubElement(channel, "link").text = "https://www.gvh.de"
    ET.SubElement(channel, "description").text = f"Nächste Abfahrten am {STOP_NAME} (Triple-API Fallback)"
    ET.SubElement(channel, "language").text = "de-de"

    if not departures:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = "Aktuell keine Abfahrten verfügbar"
    else:
        for dep in departures:
            item = ET.SubElement(channel, "item")
            line = dep.get("line", {}).get("name", "---")
            direction = dep.get("direction", "---")
            
            when_dt = parse_iso_time(dep.get("when") or dep.get("plannedWhen"))
            when_str = format_time(when_dt)
            
            delay = dep.get("delay", 0)
            delay_str = f" (+{delay//60})" if delay and delay > 0 else ""
            platform = dep.get("platform") or dep.get("plannedPlatform") or ""
            platform_str = f" Gl.{platform}" if platform else ""
            
            ET.SubElement(item, "title").text = f"{when_str}{delay_str} {line} -> {direction}{platform_str}"
            
            desc_text = "Keine weiteren Halte verfügbar"
            trip_id = dep.get("tripId")
            if trip_id and trip_base_url:
                stops = fetch_stopovers(trip_id, trip_base_url)
                if stops:
                    stop_lines = []
                    for s in stops:
                        s_name = s.get("stop", {}).get("name", "---")
                        s_time = format_time(parse_iso_time(s.get("arrival") or s.get("plannedArrival")))
                        stop_lines.append(f"{s_time} {s_name}")
                    desc_text = "\n".join(stop_lines)
            
            ET.SubElement(item, "description").text = desc_text
    
    rough_string = ET.tostring(rss, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

@app.route("/")
def index():
    return "<h1>RSS-Feed Wennigsen Bahnhof</h1><p><a href='/feed.rss'>Zum RSS-Feed</a></p>"

@app.route("/feed.rss")
def rss_feed():
    feed = generate_rss_feed()
    return Response(feed, mimetype="application/rss+xml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
