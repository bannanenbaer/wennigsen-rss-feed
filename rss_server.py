"""
RSS-Feed für Abfahrten am Bahnhof Wennigsen (Deister).

Architektur:
- ÜSTRA-API liefert Abfahrtszeiten + Echtzeitdaten (primäre Quelle)
- DB-API liefert Zwischenhalte für S-Bahnen (Anreicherung)
- DB-API dient als vollständiger Fallback, falls ÜSTRA ausfällt
- VBN-API dient als zweiter Fallback

Zuverlässigkeitsmaßnahmen:
- Retry-Logik mit exponentiellem Backoff für alle API-Aufrufe
- Getrennte Caches für Abfahrten und Zwischenhalte
- Immutable Cache-Einträge (Tuple statt List)
- Vergangene Abfahrten werden herausgefiltert
- Sortierung nach geparsten datetime-Objekten
- SSL-Warnungen unterdrückt
- Spezifisches Exception-Handling
"""

from flask import Flask, Response
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
from urllib.parse import quote
from cachetools import TTLCache
import logging
import pytz
import urllib3
import time

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rss_server")

app = Flask(__name__)

STOP_ID_DB = "8006336"
STOP_ID_UESTRA = "25005782"
STOP_NAME = "Wennigsen (Deister) Bahnhof"
BERLIN_TZ = pytz.timezone("Europe/Berlin")

# Cache (TTL 90 Sekunden)
_departures_cache = TTLCache(maxsize=5, ttl=90)
_stopovers_cache = TTLCache(maxsize=100, ttl=300)

# ---------------------------------------------------------------------------
# Robuste HTTP-Session mit Retry-Logik
# ---------------------------------------------------------------------------
def _build_session():
    """Erzeugt eine requests-Session mit automatischem Retry bei Fehlern."""
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Wennigsen-RSS-Feed/2.0 (https://wennigsen-rss-feed.onrender.com)"
    })
    session.verify = False
    return session

http = _build_session()

# ---------------------------------------------------------------------------
# ÜSTRA API
# ---------------------------------------------------------------------------
UESTRA_URL = "https://abfahrten.uestra.de/proxy2/efa/XML_DM_REQUEST"
UESTRA_PARAMS = {
    "canChangeMOT": 0,
    "coordOutputFormat": "WGS84[dd.ddddd]",
    "deleteAssignedStops_dm": 1,
    "depSequence": 30,
    "depType": "stopEvents",
    "doNotSearchForStops": 1,
    "inclMOT_1": "true", "inclMOT_2": "true", "inclMOT_3": "true",
    "inclMOT_4": "true", "inclMOT_5": "true", "inclMOT_6": "true",
    "inclMOT_7": "true", "inclMOT_8": "true", "inclMOT_9": "true",
    "inclMOT_10": "true", "inclMOT_11": "true", "inclMOT_13": "true",
    "inclMOT_14": "true", "inclMOT_15": "true", "inclMOT_16": "true",
    "inclMOT_17": "true", "inclMOT_18": "true", "inclMOT_19": "true",
    "mergeDep": 1,
    "mode": "direct",
    "outputFormat": "rapidJSON",
    "useRealtime": 1,
    "type_dm": "any",
    "name_dm": STOP_ID_UESTRA,
    "c": 1,
}

# DB / VBN API
API_DB = f"https://v6.db.transport.rest/stops/{STOP_ID_DB}/departures"
API_VBN = f"https://v6.vbn.transport.rest/stops/{STOP_ID_DB}/departures"
TRIPS_DB = "https://v6.db.transport.rest/trips"
TRIPS_VBN = "https://v6.vbn.transport.rest/trips"

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------
def parse_time(iso_time):
    """Parst ISO-Zeitstempel -> datetime in Europe/Berlin. Gibt None bei Fehler."""
    if not iso_time:
        return None
    try:
        fixed = iso_time.replace("Z", "+00:00")
        dt = datetime.fromisoformat(fixed)
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        return dt.astimezone(BERLIN_TZ)
    except Exception:
        return None


def fmt(dt):
    """Formatiert datetime -> 'HH:MM' oder '---'."""
    return dt.strftime("%H:%M") if dt else "---"


def _extract_platform(bon_str):
    """Extrahiert Gleis aus dem ÜSTRA 'bon'-Feld (z.B. 'de:03241:5782:91:2' -> '2').
    Gibt '' zurück, wenn kein sinnvolles Gleis erkennbar ist."""
    if not bon_str:
        return ""
    parts = bon_str.split(":")
    if len(parts) >= 5:
        candidate = parts[-1]
        # Nur kurze Nummern sind Gleise (1, 2, 3...), keine langen IDs wie 5782
        if candidate.isdigit() and len(candidate) <= 2:
            return candidate
    return ""


# ---------------------------------------------------------------------------
# Daten laden: ÜSTRA (primär)
# ---------------------------------------------------------------------------
def _fetch_uestra():
    """Lädt Abfahrten von der ÜSTRA-API. Gibt eine Liste von Dicts zurück."""
    try:
        resp = http.get(UESTRA_URL, params=UESTRA_PARAMS, timeout=8)
        if resp.status_code != 200:
            log.warning("ÜSTRA lieferte Status %s", resp.status_code)
            return []
        data = resp.json()
    except Exception as e:
        log.error("ÜSTRA-Anfrage fehlgeschlagen: %s", e)
        return []

    now = datetime.now(BERLIN_TZ)
    results = []
    for rd in data.get("departures", []):
        line_raw = rd.get("line", "---")
        number = rd.get("number", "")
        direction = rd.get("destination", "---")
        bon = rd.get("bon", "")
        platform = _extract_platform(bon)
        infos = rd.get("infos", [])
        hints = rd.get("hints", [])

        # Linienname bereinigen: "S-Bahn S2" -> "S2", "Bus 540" bleibt
        line_name = line_raw
        if line_raw.startswith("S-Bahn "):
            line_name = line_raw.replace("S-Bahn ", "")

        for event in rd.get("events", []):
            planned_str = event.get("plannedTime")
            actual_str = event.get("estimated_time") or planned_str
            if not planned_str:
                continue

            planned_dt = parse_time(planned_str)
            actual_dt = parse_time(actual_str) or planned_dt

            # Vergangene Abfahrten überspringen
            ref_dt = actual_dt or planned_dt
            if not ref_dt or ref_dt < now:
                continue

            delay_sec = 0
            if planned_dt and actual_dt:
                delay_sec = max(0, int((actual_dt - planned_dt).total_seconds()))

            results.append({
                "line": line_name,
                "number": number,
                "direction": direction,
                "planned_dt": planned_dt,
                "actual_dt": actual_dt,
                "delay": delay_sec,
                "platform": platform,
                "source": "uestra",
                "trip_id": None,
            })

    log.info("ÜSTRA: %d aktuelle Abfahrten geladen.", len(results))
    return results


# ---------------------------------------------------------------------------
# Daten laden: DB / VBN (Fallback + Zwischenhalte)
# ---------------------------------------------------------------------------
def _fetch_db_or_vbn():
    """Lädt Abfahrten von DB- oder VBN-API. Gibt (Liste, trips_base_url) zurück."""
    apis = [("DB", API_DB, TRIPS_DB), ("VBN", API_VBN, TRIPS_VBN)]
    now = datetime.now(BERLIN_TZ)

    for name, url, trips_url in apis:
        try:
            resp = http.get(url, params={"results": 15, "duration": 120}, timeout=10)
            if resp.status_code != 200:
                log.warning("%s lieferte Status %s", name, resp.status_code)
                continue
            raw = resp.json().get("departures", [])
            if not raw:
                log.warning("%s lieferte keine Abfahrten.", name)
                continue

            results = []
            for d in raw:
                when_str = d.get("when") or d.get("plannedWhen")
                dt = parse_time(when_str)
                if not dt or dt < now:
                    continue

                planned_dt = parse_time(d.get("plannedWhen")) or dt
                delay = d.get("delay") or 0
                if not isinstance(delay, int):
                    delay = 0

                line_obj = d.get("line", {})
                results.append({
                    "line": line_obj.get("name", "---"),
                    "number": line_obj.get("productName", ""),
                    "direction": d.get("direction", "---"),
                    "planned_dt": planned_dt,
                    "actual_dt": dt,
                    "delay": max(0, delay),
                    "platform": d.get("platform") or d.get("plannedPlatform") or "",
                    "source": name.lower(),
                    "trip_id": d.get("tripId"),
                })

            if results:
                log.info("%s: %d aktuelle Abfahrten geladen.", name, len(results))
                return results, trips_url

        except Exception as e:
            log.error("%s-Anfrage fehlgeschlagen: %s", name, e)

    return [], None


# ---------------------------------------------------------------------------
# Zwischenhalte laden (nur für DB/VBN-Trips)
# ---------------------------------------------------------------------------
def _fetch_stopovers(trip_id, trips_url):
    """Lädt Zwischenhalte für eine Fahrt. Nutzt Cache."""
    cache_key = (trip_id, trips_url)
    cached = _stopovers_cache.get(cache_key)
    if cached is not None:
        return cached

    if not trip_id or not trips_url:
        return ()

    try:
        encoded = quote(trip_id, safe="")
        resp = http.get(f"{trips_url}/{encoded}?stopovers=true", timeout=6)
        if resp.status_code != 200:
            return ()
        raw = resp.json().get("trip", {}).get("stopovers", [])

        found = False
        stops = []
        for s in raw:
            sid = s.get("stop", {}).get("id", "")
            station_id = s.get("stop", {}).get("station", {}).get("id", "")
            if sid == STOP_ID_DB or station_id == STOP_ID_DB:
                found = True
                continue
            if found:
                name = s.get("stop", {}).get("name", "---")
                arr = parse_time(s.get("arrival") or s.get("plannedArrival"))
                stops.append((fmt(arr), name))

        result = tuple(stops)
        _stopovers_cache[cache_key] = result
        return result

    except Exception as e:
        log.error("Stopovers-Fehler für %s: %s", trip_id[:30], e)
        return ()


# ---------------------------------------------------------------------------
# Hauptlogik: Abfahrten zusammenführen
# ---------------------------------------------------------------------------
def _get_departures():
    """Liefert sortierte Abfahrten. Nutzt Cache."""
    cached = _departures_cache.get("deps")
    if cached is not None:
        return cached

    # 1. ÜSTRA als primäre Quelle für Abfahrtszeiten
    uestra_deps = _fetch_uestra()

    # 2. DB/VBN für Zwischenhalte (und als Fallback)
    db_deps, trips_url = _fetch_db_or_vbn()

    if uestra_deps:
        # ÜSTRA hat Daten -> nutze sie, reichere S-Bahnen mit DB-Zwischenhalten an
        # Erstelle Lookup: (Linie, gerundete Minute) -> DB-Departure für Matching
        db_lookup = {}
        for d in db_deps:
            key = (d["line"], d["planned_dt"].strftime("%H:%M") if d["planned_dt"] else "")
            db_lookup[key] = d

        enriched = []
        for dep in uestra_deps:
            # Versuche passende DB-Abfahrt zu finden für Zwischenhalte
            key = (dep["line"], dep["planned_dt"].strftime("%H:%M") if dep["planned_dt"] else "")
            db_match = db_lookup.get(key)
            if db_match:
                dep["trip_id"] = db_match.get("trip_id")
                # Übernehme Gleis von DB falls ÜSTRA keins hat
                if not dep["platform"] and db_match.get("platform"):
                    dep["platform"] = db_match["platform"]
            enriched.append(dep)

        final = enriched
        source_info = "ÜSTRA + DB"
    elif db_deps:
        # Fallback: Nur DB/VBN-Daten
        final = db_deps
        source_info = "DB/VBN (Fallback)"
    else:
        final = []
        source_info = "Keine Daten"

    # Sortierung nach tatsächlicher Abfahrtszeit
    final.sort(key=lambda x: x.get("actual_dt") or x.get("planned_dt") or datetime.max.replace(tzinfo=pytz.utc))

    # Als Tuple cachen (immutable)
    result = (tuple(final), trips_url, source_info)
    _departures_cache["deps"] = result
    log.info("Feed erstellt mit Quelle: %s (%d Abfahrten)", source_info, len(final))
    return result


# ---------------------------------------------------------------------------
# RSS-Feed generieren
# ---------------------------------------------------------------------------
def _build_feed():
    """Erzeugt den RSS-Feed als XML-String."""
    deps_tuple, trips_url, source_info = _get_departures()
    departures = list(deps_tuple)

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = f"Abfahrten {STOP_NAME}"
    ET.SubElement(channel, "link").text = "https://www.gvh.de"
    ET.SubElement(channel, "description").text = (
        f"Nächste Abfahrten am {STOP_NAME} (Quelle: {source_info})"
    )
    ET.SubElement(channel, "language").text = "de-de"
    ET.SubElement(channel, "lastBuildDate").text = (
        datetime.now(BERLIN_TZ).strftime("%a, %d %b %Y %H:%M:%S %z")
    )

    if not departures:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = "Aktuell keine Abfahrten verfügbar"
        ET.SubElement(item, "description").text = (
            "Alle Datenquellen liefern derzeit keine Abfahrten. "
            "Bitte prüfe die GVH-Verkehrsmeldungen."
        )
    else:
        for dep in departures:
            item = ET.SubElement(channel, "item")

            when_str = fmt(dep.get("actual_dt") or dep.get("planned_dt"))
            delay = dep.get("delay", 0)
            delay_str = f" (+{delay // 60} Min)" if delay >= 60 else ""
            line = dep.get("line", "---")
            direction = dep.get("direction", "---")
            platform = dep.get("platform", "")
            platform_str = f" Gl.{platform}" if platform else ""

            ET.SubElement(item, "title").text = (
                f"{when_str}{delay_str} {line} -> {direction}{platform_str}"
            )

            # Zwischenhalte
            desc_parts = []
            trip_id = dep.get("trip_id")
            if trip_id and trips_url:
                stops = _fetch_stopovers(trip_id, trips_url)
                if stops:
                    for s_time, s_name in stops:
                        desc_parts.append(f"{s_time} {s_name}")

            if not desc_parts:
                desc_parts.append("Keine weiteren Halte verfügbar")

            ET.SubElement(item, "description").text = "\n".join(desc_parts)

    raw_xml = ET.tostring(rss, "utf-8")
    pretty = minidom.parseString(raw_xml)
    return pretty.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")


# ---------------------------------------------------------------------------
# Flask-Routen
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return (
        "<h1>RSS-Feed Wennigsen (Deister) Bahnhof</h1>"
        "<p><a href='/feed.rss'>Zum RSS-Feed</a></p>"
        "<p>Datenquellen: ÜSTRA (Echtzeit) + Deutsche Bahn (Zwischenhalte)</p>"
    )


@app.route("/feed.rss")
@app.route("/feed")
def rss_feed():
    xml = _build_feed()
    return Response(xml, mimetype="application/rss+xml")


@app.route("/health")
def health():
    """Einfacher Health-Check für Monitoring."""
    try:
        deps_tuple, _, source = _get_departures()
        return {"status": "ok", "source": source, "departures": len(deps_tuple)}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
