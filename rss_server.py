from flask import Flask, Response
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
from urllib.parse import quote
from cachetools import cached, TTLCache
import logging

# Konfiguriere Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

STOP_ID = "8006336"
STOP_NAME = "Wennigsen (Deister) Bahnhof"
API_URL = f"https://v6.db.transport.rest/stops/{STOP_ID}/departures"
TRIPS_URL = "https://v6.db.transport.rest/trips"

# Cache-Konfiguration (z.B. 2 Minuten TTL)
cache = TTLCache(maxsize=100, ttl=120)

HEADERS = {
    "User-Agent": "Wennigsen-RSS-Feed-Bot/1.0 (https://wennigsen-rss-feed.onrender.com)"
}

@cached(cache)
def fetch_departures(results=10, duration=120):
    params = {"results": results, "duration": duration, "language": "de", "pretty": "false"}
    try:
        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=10, verify=False)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully fetched {len(data.get('departures', []))} departures.")
        return data.get("departures", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching departures from {API_URL}: {e}")
        return []
    except ValueError as e: # JSONDecodeError
        logging.error(f"JSON decoding error for departures from {API_URL}: {e}")
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
        logging.info(f"Successfully fetched {len(stopovers)} stopovers for trip {trip_id}.")
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
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching stopovers for trip {trip_id} from {url}: {e}")
        return []
    except ValueError as e: # JSONDecodeError
        logging.error(f"JSON decoding error for stopovers for trip {trip_id} from {url}: {e}")
        return []

def format_time(iso_time):
    if not iso_time:
        return "---"
    try:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00")) # Handle 'Z' for UTC
        return dt.strftime("%H:%M")
    except ValueError:
        return "---"

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
        arrival = stop.get("arrival")
        arrival_time = format_time(arrival)
        delay = stop.get("arrivalDelay", 0)
        delay_str = ""
        if delay and delay > 0:
            delay_min = delay // 60
            delay_str = f" (+{delay_min})"
        lines.append(f"{arrival_time}{delay_str} {stop_name}")
    return "\n".join(lines)

def generate_rss_feed():
    departures = fetch_departures(results=10, duration=120)
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    title = ET.SubElement(channel, "title")
    title.text = f"Abfahrten {STOP_NAME}"
    link = ET.SubElement(channel, "link")
    link.text = "https://www.gvh.de"
    description = ET.SubElement(channel, "description")
    description.text = f"Nächste Abfahrten am {STOP_NAME}"
    language = ET.SubElement(channel, "language")
    language.text = "de-de"

    if not departures:
        item = ET.SubElement(channel, "item")
        item_title = ET.SubElement(item, "title")
        item_title.text = "Keine Abfahrten verfügbar"
    else:
        for dep in departures:
            item = ET.SubElement(channel, "item")
            line_name = dep.get("line", {}).get("name", "---")
            direction = dep.get("direction", "---")
            when = format_time(dep.get("when"))
            delay = dep.get("delay", 0)
            delay_str = format_delay(delay)
            platform = dep.get("platform") or dep.get("plannedPlatform") or ""
            platform_str = f" Gl.{platform}" if platform else ""
            item_title = ET.SubElement(item, "title")
            item_title.text = f"{when}{delay_str} {line_name} -> {direction}{platform_str}"
            item_desc = ET.SubElement(item, "description")
            trip_id = dep.get("tripId", "")
            stop_id = dep.get("stop", {}).get("id", STOP_ID)
            if trip_id:
                stopovers = fetch_stopovers(trip_id, stop_id)
                stopovers_text = format_stopovers(stopovers)
                item_desc.text = stopovers_text
            else:
                item_desc.text = f"Linie: {line_name} | Richtung: {direction}"
    
    # Pretty print the XML
    rough_string = ET.tostring(rss, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

@app.route("/")
def index():
    return "<h1>RSS-Feed Wennigsen Bahnhof</h1><p><a href=\'/feed.rss\'>Zum RSS-Feed</a></p>"

@app.route("/feed.rss")
@app.route("/feed")
def rss_feed():
    feed = generate_rss_feed()
    return Response(feed, mimetype="application/rss+xml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
