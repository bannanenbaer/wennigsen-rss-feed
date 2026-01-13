from flask import Flask, Response
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom

app = Flask(__name__)

STOP_ID = "8006336"
STOP_NAME = "Wennigsen (Deister) Bahnhof"
API_URL = f"https://v6.db.transport.rest/stops/{STOP_ID}/departures"

def fetch_departures(results=10, duration=120):
    params = {"results": results, "duration": duration, "language": "de", "pretty": "false"}
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("departures", [])
    except:
        return []

def format_time(iso_time):
    if not iso_time:
        return "---"
    try:
        dt = datetime.fromisoformat(iso_time)
        return dt.strftime("%H:%M")
    except:
        return "---"

def format_delay(delay):
    if delay is None or delay == 0:
        return ""
    minutes = delay // 60
    if minutes > 0:
        return f" (+{minutes})"
    return ""

def generate_rss_feed():
    departures = fetch_departures(results=10, duration=120)
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    title = ET.SubElement(channel, "title")
    title.text = f"Abfahrten {STOP_NAME}"
    link = ET.SubElement(channel, "link")
    link.text = "https://www.gvh.de"
    description = ET.SubElement(channel, "description")
    description.text = f"Naechste Abfahrten am {STOP_NAME}"
    language = ET.SubElement(channel, "language")
    language.text = "de-de"
    if not departures:
        item = ET.SubElement(channel, "item")
        item_title = ET.SubElement(item, "title")
        item_title.text = "Keine Abfahrten verfuegbar"
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
            item_desc.text = f"Linie: {line_name} | Richtung: {direction} | Abfahrt: {when}{delay_str}"
    xml_str = ET.tostring(rss, encoding="unicode")
    dom = minidom.parseString(xml_str)
    return dom.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

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
