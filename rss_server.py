"""
RSS-Feed für Abfahrten am Bahnhof Wennigsen (Deister).

Architektur:
- ÜSTRA-API liefert Abfahrtszeiten + Echtzeitdaten (primäre Quelle)
- DB-API liefert Zwischenhalte, Ausfälle, Remarks (Anreicherung)
- DB-API dient als vollständiger Fallback, falls ÜSTRA ausfällt
- VBN-API dient als zweiter Fallback

Titel-Format:
  Pünktlich:    "S1 10:58 -> Haste Bahnhof Gl.2"
  Verspätet:    "S1 11:02 +4 Min -> Haste Bahnhof Gl.2"
  Ausfall:      "[FÄLLT AUS] S1 10:58 -> Haste Bahnhof Gl.2"

Details enthalten:
  - Störungsgrund (z.B. "Grund: Personalausfall")
  - Zwischenhalte mit Uhrzeiten
  - Hinweise (z.B. "Fahrradmitnahme begrenzt möglich")

Zuverlässigkeitsmaßnahmen:
- Retry-Logik mit exponentiellem Backoff
- Getrennte Caches (Abfahrten 90s, Stopovers 5min)
- Immutable Cache-Einträge (Tuple)
- Vergangene Abfahrten werden herausgefiltert
- Sortierung nach geparsten datetime-Objekten
- Tolerantes Matching (±2 Minuten)
- SSL-Warnungen unterdrückt
"""

from flask import Flask, Response
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from xml.dom import minidom
from urllib.parse import quote
from cachetools import TTLCache
import logging
import pytz
import urllib3

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("rss_server")

app = Flask(__name__)

STOP_ID_DB = "8006336"
STOP_ID_UESTRA = "25005782"
STOP_NAME = "Wennigsen (Deister) Bahnhof"
BERLIN_TZ = pytz.timezone("Europe/Berlin")

# Caches
_departures_cache = TTLCache(maxsize=5, ttl=90)
_stopovers_cache = TTLCache(maxsize=100, ttl=300)

# ---------------------------------------------------------------------------
# Robuste HTTP-Session
# ---------------------------------------------------------------------------
def _build_session():
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
        "User-Agent": "Wennigsen-RSS-Feed/2.1 "
                      "(https://wennigsen-rss-feed.onrender.com)"
    })
    session.verify = False
    return session

http = _build_session()

# ---------------------------------------------------------------------------
# API-URLs
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

API_DB = f"https://v6.db.transport.rest/stops/{STOP_ID_DB}/departures"
API_VBN = f"https://v6.vbn.transport.rest/stops/{STOP_ID_DB}/departures"
TRIPS_DB = "https://v6.db.transport.rest/trips"
TRIPS_VBN = "https://v6.vbn.transport.rest/trips"

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------
def parse_time(iso_time):
    """ISO-Zeitstempel -> datetime in Europe/Berlin. None bei Fehler."""
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
    """datetime -> 'HH:MM'."""
    return dt.strftime("%H:%M") if dt else "---"


def _extract_platform(bon_str):
    """Gleis aus ÜSTRA 'bon'-Feld extrahieren."""
    if not bon_str:
        return ""
    parts = bon_str.split(":")
    if len(parts) >= 5:
        candidate = parts[-1]
        if candidate.isdigit() and len(candidate) <= 2:
            return candidate
    return ""


def _clean_line_name(raw):
    """'S-Bahn S2' -> 'S2', 'Bus 540' bleibt."""
    if raw.startswith("S-Bahn "):
        return raw[7:]
    return raw


# ---------------------------------------------------------------------------
# ÜSTRA API (primäre Abfahrtszeiten)
# ---------------------------------------------------------------------------
def _fetch_uestra():
    """Lädt Abfahrten + Hinweise von der ÜSTRA-API."""
    try:
        resp = http.get(UESTRA_URL, params=UESTRA_PARAMS, timeout=8)
        if resp.status_code != 200:
            log.warning("ÜSTRA Status %s", resp.status_code)
            return []
        data = resp.json()
    except Exception as e:
        log.error("ÜSTRA fehlgeschlagen: %s", e)
        return []

    now = datetime.now(BERLIN_TZ)
    results = []

    for rd in data.get("departures", []):
        line_name = _clean_line_name(rd.get("line", "---"))
        number = rd.get("number", "")
        direction = rd.get("destination", "---")
        platform = _extract_platform(rd.get("bon", ""))

        # Hinweise sammeln (z.B. Fahrradmitnahme, Niederflurbus)
        hints_text = []
        for h in rd.get("hints", []):
            content = h.get("content", "")
            htype = h.get("type", "")
            if htype != "VehicleType" and content:
                hints_text.append(content)

        # Störungsmeldungen aus infos-Feld
        disruptions = []
        for info in rd.get("infos", []):
            title = info.get("title", "")
            text = info.get("text", "")
            if title:
                disruptions.append(title)
            elif text:
                disruptions.append(text)

        for event in rd.get("events", []):
            planned_str = event.get("plannedTime")
            actual_str = event.get("estimated_time") or planned_str
            if not planned_str:
                continue

            planned_dt = parse_time(planned_str)
            actual_dt = parse_time(actual_str) or planned_dt

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
                "cancelled": False,
                "remarks": disruptions[:],
                "hints": hints_text[:],
                "source": "uestra",
                "trip_id": None,
            })

    log.info("ÜSTRA: %d Abfahrten geladen.", len(results))
    return results


# ---------------------------------------------------------------------------
# DB / VBN API (Fallback + Anreicherung)
# ---------------------------------------------------------------------------
def _fetch_db_or_vbn():
    """Lädt Abfahrten von DB/VBN inkl. Ausfälle und Remarks."""
    apis = [("DB", API_DB, TRIPS_DB), ("VBN", API_VBN, TRIPS_VBN)]
    now = datetime.now(BERLIN_TZ)

    for name, url, trips_url in apis:
        try:
            resp = http.get(
                url,
                params={"results": 30, "duration": 180,
                        "remarks": "true", "language": "de"},
                timeout=10,
            )
            if resp.status_code != 200:
                log.warning("%s Status %s", name, resp.status_code)
                continue
            raw = resp.json().get("departures", [])
            if not raw:
                log.warning("%s keine Abfahrten.", name)
                continue

            results = []
            for d in raw:
                cancelled = d.get("cancelled", False)
                when_str = d.get("when") or d.get("plannedWhen")
                planned_when = d.get("plannedWhen")

                planned_dt = parse_time(planned_when)
                actual_dt = parse_time(when_str) if when_str else planned_dt

                # Bei Ausfällen: when ist oft None, nutze plannedWhen
                if not actual_dt:
                    actual_dt = planned_dt
                if not planned_dt:
                    continue

                # Vergangene nur überspringen wenn nicht ausgefallen
                ref = actual_dt or planned_dt
                if not cancelled and ref < now:
                    continue
                # Ausgefallene in der Vergangenheit auch überspringen
                if cancelled and planned_dt < now:
                    continue

                delay = d.get("delay") or 0
                if not isinstance(delay, (int, float)):
                    delay = 0

                # Remarks auswerten -> Störungsgründe extrahieren
                remarks_list = []
                for rm in d.get("remarks", []):
                    rm_type = rm.get("type", "")
                    rm_code = rm.get("code", "")
                    rm_text = rm.get("text", "") or rm.get("summary", "")
                    # Nur relevante Meldungen (warning/status), keine Hinweise
                    if rm_type in ("warning", "status") and rm_text:
                        remarks_list.append(rm_text)

                line_obj = d.get("line", {})
                results.append({
                    "line": line_obj.get("name", "---"),
                    "number": line_obj.get("productName", ""),
                    "direction": d.get("direction", "---"),
                    "planned_dt": planned_dt,
                    "actual_dt": actual_dt,
                    "delay": max(0, int(delay)),
                    "platform": d.get("platform")
                                or d.get("plannedPlatform") or "",
                    "cancelled": cancelled,
                    "remarks": remarks_list,
                    "hints": [],
                    "source": name.lower(),
                    "trip_id": d.get("tripId"),
                })

            if results:
                log.info("%s: %d Abfahrten geladen.", name, len(results))
                return results, trips_url

        except Exception as e:
            log.error("%s fehlgeschlagen: %s", name, e)

    return [], None


# ---------------------------------------------------------------------------
# Zwischenhalte laden
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
        resp = http.get(
            f"{trips_url}/{encoded}",
            params={"stopovers": "true", "remarks": "true", "language": "de"},
            timeout=6,
        )
        if resp.status_code != 200:
            log.warning("Stopovers Status %s für Trip %s", resp.status_code, trip_id[:40])
            return ()

        trip_data = resp.json().get("trip", {})
        raw = trip_data.get("stopovers", [])

        # Störungsgründe aus Trip-Remarks extrahieren
        trip_remarks = []
        for rm in trip_data.get("remarks", []):
            rm_type = rm.get("type", "")
            rm_text = rm.get("text", "") or rm.get("summary", "")
            if rm_type in ("warning", "status") and rm_text:
                trip_remarks.append(rm_text)

        found = False
        stops = []
        for s in raw:
            sid = s.get("stop", {}).get("id", "")
            station_id = (s.get("stop", {})
                           .get("station", {})
                           .get("id", ""))
            if sid == STOP_ID_DB or station_id == STOP_ID_DB:
                found = True
                continue
            if found:
                sname = s.get("stop", {}).get("name", "---")
                arr = parse_time(
                    s.get("arrival") or s.get("plannedArrival")
                )
                s_cancelled = s.get("cancelled", False)
                stops.append((fmt(arr), sname, s_cancelled))

        result = (tuple(stops), tuple(trip_remarks))
        _stopovers_cache[cache_key] = result
        return result

    except Exception as e:
        log.error("Stopovers-Fehler: %s", e)
        return ()


# ---------------------------------------------------------------------------
# Hauptlogik: Abfahrten zusammenführen
# ---------------------------------------------------------------------------
def _get_departures():
    """Liefert sortierte Abfahrten (aus Cache oder frisch)."""
    cached = _departures_cache.get("deps")
    if cached is not None:
        return cached

    uestra_deps = _fetch_uestra()
    db_deps, trips_url = _fetch_db_or_vbn()

    if uestra_deps:
        # Tolerantes Matching: (Linie, Minute ±2) -> DB-Departure
        db_lookup = {}
        for d in db_deps:
            if d["planned_dt"]:
                key = (d["line"], d["planned_dt"].strftime("%H:%M"))
                db_lookup[key] = d

        enriched = []
        for dep in uestra_deps:
            if not dep["planned_dt"]:
                enriched.append(dep)
                continue

            # Exaktes Matching
            key = (dep["line"], dep["planned_dt"].strftime("%H:%M"))
            db_match = db_lookup.get(key)

            # Tolerantes Matching (±2 Minuten)
            if not db_match:
                for offset in [-60, 60, -120, 120]:
                    alt_time = dep["planned_dt"] + timedelta(seconds=offset)
                    alt_key = (dep["line"], alt_time.strftime("%H:%M"))
                    db_match = db_lookup.get(alt_key)
                    if db_match:
                        break

            if db_match:
                dep["trip_id"] = db_match.get("trip_id")
                # Gleis von DB übernehmen falls ÜSTRA keins hat
                if not dep["platform"] and db_match.get("platform"):
                    dep["platform"] = db_match["platform"]
                # Ausfall-Status von DB übernehmen
                if db_match.get("cancelled"):
                    dep["cancelled"] = True
                # Remarks von DB übernehmen
                if db_match.get("remarks"):
                    dep["remarks"] = list(
                        set(dep["remarks"] + db_match["remarks"])
                    )

            enriched.append(dep)

        # Ausgefallene DB-Abfahrten hinzufügen, die ÜSTRA nicht kennt
        uestra_keys = set()
        for dep in enriched:
            if dep["planned_dt"]:
                uestra_keys.add(
                    (dep["line"], dep["planned_dt"].strftime("%H:%M"))
                )
        for d in db_deps:
            if d.get("cancelled") and d["planned_dt"]:
                key = (d["line"], d["planned_dt"].strftime("%H:%M"))
                if key not in uestra_keys:
                    enriched.append(d)

        final = enriched
        source_info = "ÜSTRA + DB"
    elif db_deps:
        final = db_deps
        source_info = "DB/VBN (Fallback)"
    else:
        final = []
        source_info = "Keine Daten"

    # Sortierung: Ausfälle ans Ende ihrer Zeitgruppe, sonst nach actual_dt
    _max_dt = datetime.max.replace(tzinfo=pytz.utc)
    final_sorted = sorted(
        final,
        key=lambda x: (
            x.get("actual_dt") or x.get("planned_dt") or _max_dt,
            1 if x.get("cancelled") else 0,
        ),
    )

    result = (tuple(final_sorted), trips_url, source_info)
    _departures_cache["deps"] = result
    log.info("Feed: %s (%d Abfahrten)", source_info, len(final_sorted))
    return result


# ---------------------------------------------------------------------------
# RSS-Feed generieren
# ---------------------------------------------------------------------------
def _build_feed():
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
        ET.SubElement(item, "title").text = (
            "Aktuell keine Abfahrten verfügbar"
        )
        ET.SubElement(item, "description").text = (
            "Alle Datenquellen liefern derzeit keine Abfahrten. "
            "Bitte prüfe die GVH-Verkehrsmeldungen."
        )
    else:
        for dep in departures:
            item = ET.SubElement(channel, "item")

            line = dep.get("line", "---")
            direction = dep.get("direction", "---")
            platform = dep.get("platform", "")
            platform_str = f" Gl.{platform}" if platform else ""
            cancelled = dep.get("cancelled", False)
            delay = dep.get("delay", 0)
            planned_dt = dep.get("planned_dt")
            actual_dt = dep.get("actual_dt")

            # --- TITEL ---
            if cancelled:
                # Format: [FÄLLT AUS] S1 10:58 -> Haste Gl.2
                title = (
                    f"[FÄLLT AUS] {line} {fmt(planned_dt)} "
                    f"-> {direction}{platform_str}"
                )
            elif delay >= 60:
                # Format: S1 11:02 +4 Min -> Haste Gl.2
                delay_min = delay // 60
                title = (
                    f"{line} {fmt(actual_dt)} +{delay_min} Min "
                    f"-> {direction}{platform_str}"
                )
            else:
                # Format: S1 10:58 -> Haste Gl.2
                title = (
                    f"{line} {fmt(actual_dt or planned_dt)} "
                    f"-> {direction}{platform_str}"
                )

            ET.SubElement(item, "title").text = title

            # --- BESCHREIBUNG / DETAILS ---
            desc_parts = []

            # 1. Störungsgründe (aus DB-Remarks)
            remarks = dep.get("remarks", [])

            # 2. Zwischenhalte + Trip-Remarks laden
            trip_id = dep.get("trip_id")
            trip_remarks = []
            stopover_lines = []
            if trip_id and trips_url and not cancelled:
                so_result = _fetch_stopovers(trip_id, trips_url)
                if so_result and len(so_result) == 2:
                    stops, t_remarks = so_result
                    trip_remarks = list(t_remarks)
                    for s_time, s_name, s_cancelled in stops:
                        if s_cancelled:
                            stopover_lines.append(
                                f"{s_time} {s_name} [entfällt]"
                            )
                        else:
                            stopover_lines.append(f"{s_time} {s_name}")

            # Alle Remarks zusammenführen (dedupliziert)
            all_remarks = list(dict.fromkeys(remarks + trip_remarks))

            # Verspätungsinfo
            if cancelled:
                desc_parts.append("*** Diese Fahrt fällt aus ***")
                if planned_dt:
                    desc_parts.append(
                        f"Geplante Abfahrt: {fmt(planned_dt)}"
                    )
            elif delay >= 60:
                delay_min = delay // 60
                desc_parts.append(
                    f"Verspätung: +{delay_min} Min "
                    f"(geplant: {fmt(planned_dt)}, "
                    f"voraussichtlich: {fmt(actual_dt)})"
                )

            # Störungsgründe
            if all_remarks:
                for rm in all_remarks:
                    desc_parts.append(f"Grund: {rm}")

            # Zwischenhalte
            if stopover_lines:
                if desc_parts:
                    desc_parts.append("")  # Leerzeile
                desc_parts.append("Zwischenhalte:")
                desc_parts.extend(stopover_lines)
            elif not cancelled:
                if desc_parts:
                    desc_parts.append("")
                desc_parts.append("Keine Zwischenhalte verfügbar")

            # Hinweise (ÜSTRA)
            hints = dep.get("hints", [])
            if hints:
                if desc_parts:
                    desc_parts.append("")
                for h in hints:
                    desc_parts.append(f"Hinweis: {h}")

            if not desc_parts:
                desc_parts.append("Keine weiteren Informationen")

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
        "<p>Datenquellen: ÜSTRA (Echtzeit) + Deutsche Bahn "
        "(Zwischenhalte &amp; Störungen)</p>"
    )


@app.route("/feed.rss")
@app.route("/feed")
def rss_feed():
    xml = _build_feed()
    return Response(xml, mimetype="application/rss+xml")


@app.route("/health")
def health():
    """Health-Check für Monitoring."""
    try:
        deps_tuple, _, source = _get_departures()
        cancelled = sum(1 for d in deps_tuple if d.get("cancelled"))
        delayed = sum(1 for d in deps_tuple if d.get("delay", 0) >= 60)
        return {
            "status": "ok",
            "source": source,
            "departures": len(deps_tuple),
            "cancelled": cancelled,
            "delayed": delayed,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
