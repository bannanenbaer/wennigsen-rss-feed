from flask import Flask, Response
import re
import requests
import threading
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta, date
from urllib.parse import quote
import logging
import pytz
import urllib3
from bs4 import BeautifulSoup
import json
import os
import sys
import urllib.parse

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

STOP_ID_DB     = "8006336"
STOP_ID_NAHVERKEHR = "25005782"
STOP_NAME      = "Wennigsen (Deister) Bahnhof"
BERLIN_TZ      = pytz.timezone("Europe/Berlin")

MAX_DEPARTURES      = 30
MAX_STOPS           = 10

# ---------------------------------------------------------------------------
# Konfigurations-State (befuellt durch load_config() beim Start)
# ---------------------------------------------------------------------------
_cfg = {}
_provider_available = {"nahverkehr": False, "db": False, "sbahn": False}


def load_config(path="config.txt"):
    """Lade config.txt und setze _cfg. Format: 'KEY: VALUE', '< ZIEL', '> ZIEL'."""
    global _cfg
    if not os.path.exists(path):
        log.warning("[Config] Keine %s gefunden – verwende interne Defaults.", path)
        return
    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("< "):
                _cfg["direction_left"] = line[2:].strip()
            elif line.startswith("> "):
                _cfg["direction_right"] = line[2:].strip()
            elif ": " in line:
                key, _, value = line.partition(": ")
                key = key.strip()
                value = value.strip()
                if key == "local_provider":
                    _cfg["local_providers"] = [d.strip() for d in value.split(";") if d.strip()]
                else:
                    _cfg[key] = value
    if not _cfg.get("stop_name"):
        log.error("[Config] 'stop_name' fehlt in %s.", path)
        sys.exit(1)
    log.info("[Config] Geladen: stop_name=%s, postal_code=%s, providers=%s",
             _cfg.get("stop_name"), _cfg.get("postal_code", "(keine)"),
             _cfg.get("local_providers", []))


def resolve_stop_ids():
    """Loese STOP_ID_DB per DB REST Locations-API auf. STOP_ID_NAHVERKEHR per EFA."""
    global STOP_ID_DB, API_DB, TRIPS_DB, STOP_NAME
    stop_name = _cfg.get("stop_name", STOP_NAME)
    postal_code = _cfg.get("postal_code", "")
    STOP_NAME = stop_name
    try:
        resp = requests.get(
            "https://v6.db.transport.rest/locations",
            params={"query": stop_name, "results": 20},
            timeout=8,
        )
        resp.raise_for_status()
        locations = resp.json()
        candidates = locations
        if postal_code:
            filtered = [
                loc for loc in locations
                if postal_code in str((loc.get("address") or {}).get("postalCode", ""))
            ]
            if filtered:
                candidates = filtered
        stop_cands = [c for c in candidates if c.get("type") in ("stop", "station")]
        best = stop_cands[0] if stop_cands else (candidates[0] if candidates else None)
        if best:
            STOP_ID_DB = str(best.get("id", STOP_ID_DB))
            log.info("[Config] STOP_ID_DB aufgeloest: %s (%s)", STOP_ID_DB, best.get("name", "?"))
        else:
            log.warning("[Config] Keine Location-Ergebnisse fuer '%s' – behalte Default.", stop_name)
    except Exception as exc:
        log.warning("[Config] Location-Suche fehlgeschlagen: %s – behalte Default.", exc)
    API_DB   = f"https://v6.db.transport.rest/stops/{STOP_ID_DB}/departures"
    TRIPS_DB = "https://v6.db.transport.rest/trips"
    log.info("[Config] Finale Stop-IDs: DB=%s, NAHVERKEHR=%s", STOP_ID_DB, STOP_ID_NAHVERKEHR)


def _extract_stop_id_from_efa(data):
    """Extrahiere Stop-ID aus EFA rapidJSON Antwort."""
    for dep in data.get("departures", []):
        stop_id = (dep.get("stop") or {}).get("id") or dep.get("stopId")
        if stop_id:
            return str(stop_id)
    return None


def _find_hafas_auth(soup, homepage_url):
    """Suche nach HAFAS AID-Token in Seitenquelltext und verlinkten JS-Dateien."""
    AID_RE = re.compile(r'"aid"\s*:\s*"([A-Za-z0-9]{10,40})"')
    for script in soup.find_all("script"):
        m = AID_RE.search(script.get_text())
        if m:
            return m.group(1)
    base = urllib.parse.urlparse(homepage_url)
    base_origin = f"{base.scheme}://{base.netloc}"
    for tag in soup.find_all("script", src=True):
        src = tag.get("src", "")
        if src.startswith("//"):
            src = base.scheme + ":" + src
        elif src.startswith("/"):
            src = base_origin + src
        elif not src.startswith("http"):
            continue
        try:
            r = requests.get(src, timeout=4, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                m = AID_RE.search(r.text)
                if m:
                    log.info("[Discovery] HAFAS Auth-Token in JS gefunden: %s", src)
                    return m.group(1)
        except Exception:
            pass
    return None


def _discover_provider(domain):
    """Crawle eine Provider-Domain und suche nach EFA-, HAFAS- und Scraping-Endpunkten."""
    result = {
        "efa_url": None, "efa_stop_id": None,
        "hafas_url": None, "hafas_auth": None,
        "scraping_url": None, "scraping_selector": None,
    }
    stop_name = _cfg.get("stop_name", STOP_NAME)
    clean_domain = domain.lstrip("http://").lstrip("https://").lstrip("www.")

    homepage_url = None
    soup = None
    all_links = set()
    for try_url in [f"https://www.{clean_domain}", f"https://{clean_domain}"]:
        try:
            r = requests.get(try_url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (RSS-Feed-Bot)"})
            if r.status_code == 200:
                homepage_url = try_url
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if href.startswith("http"):
                        all_links.add(href)
                break
        except Exception as e:
            log.debug("[Discovery] %s – Homepage-Fehler: %s", try_url, e)

    if not soup:
        log.warning("[Discovery] Domain %s nicht erreichbar.", domain)
        return result

    candidate_bases = set()
    for link in all_links:
        try:
            p = urllib.parse.urlparse(link)
            candidate_bases.add(f"{p.scheme}://{p.netloc}")
        except Exception:
            pass
    for sub in ["abfahrten", "fahrplan", "auskunft", "efa", "api", "www", ""]:
        prefix = f"{sub}." if sub else ""
        candidate_bases.add(f"https://{prefix}{clean_domain}")

    EFA_PATHS = [
        "/proxy2/efa/XML_DM_REQUEST",
        "/efa/XML_DM_REQUEST",
        "/xml_departureboard/efa/XML_DM_REQUEST",
    ]
    for base in sorted(candidate_bases):
        if result["efa_url"]:
            break
        for path in EFA_PATHS:
            url = base + path
            try:
                r = requests.get(url, params={
                    "type_dm": "any", "name_dm": stop_name,
                    "outputFormat": "rapidJSON", "depSequence": 1, "mode": "direct",
                }, timeout=5)
                if r.status_code == 200:
                    try:
                        data = r.json()
                        if "departures" in data or "stopEvents" in data:
                            result["efa_url"] = url
                            result["efa_stop_id"] = _extract_stop_id_from_efa(data)
                            log.info("[Discovery] EFA gefunden: %s (Stop-ID: %s)",
                                     url, result["efa_stop_id"])
                            break
                    except Exception:
                        pass
            except Exception:
                pass

    HAFAS_PATHS = ["/hamm", "/hamm/"]
    HAFAS_TEST = {
        "ver": "1.56", "lang": "de",
        "auth": {"type": "AID", "aid": "PLACEHOLDER"},
        "client": {"id": "TEST", "type": "WEB"},
        "formatted": False,
        "svcReqL": [{"meth": "ServerInfo", "req": {}, "id": "1"}],
    }
    for base in sorted(candidate_bases):
        if result["hafas_url"]:
            break
        for path in HAFAS_PATHS:
            url = base + path
            try:
                r = requests.post(url, json=HAFAS_TEST, timeout=5,
                                  headers={"Content-Type": "application/json"})
                if r.status_code == 200:
                    try:
                        data = r.json()
                        if "svcResL" in data or "err" in data:
                            result["hafas_url"] = url
                            result["hafas_auth"] = _find_hafas_auth(soup, homepage_url)
                            log.info("[Discovery] HAFAS gefunden: %s (Auth: %s)", url,
                                     "ja" if result["hafas_auth"] else "nicht gefunden")
                            break
                    except Exception:
                        pass
            except Exception:
                pass

    SCRAPE_SELECTORS = [
        ("li", "main-announcements__text"),
    ]
    for tag, cls in SCRAPE_SELECTORS:
        if soup.find_all(tag, class_=cls):
            result["scraping_url"] = homepage_url
            result["scraping_selector"] = f"{tag}.{cls}"
            log.info("[Discovery] Scraping-Ziel gefunden: %s @ %s",
                     result["scraping_selector"], homepage_url)
            break
    if not result["scraping_url"]:
        for keyword in ["announcement", "meldung", "stoerung", "alert"]:
            found = soup.find_all(
                attrs={"class": lambda c, kw=keyword: bool(c and kw in " ".join(c).lower())}
            )
            if found:
                result["scraping_url"] = homepage_url
                result["scraping_selector"] = f"[class*='{keyword}']"
                log.info("[Discovery] Scraping (allg.) gefunden: %s @ %s",
                         result["scraping_selector"], homepage_url)
                break

    return result


def discover_providers(domains):
    """Analysiere alle konfigurierten Provider-Domains und befuelle _cfg."""
    global STOP_ID_NAHVERKEHR, NAHVERKEHR_URL, _HAFAS_URL
    _cfg.setdefault("providers_discovered", {})
    _cfg.setdefault("providers_scraping", [])
    efa_set = False
    hafas_set = False
    for domain in domains:
        log.info("[Discovery] Analysiere: %s", domain)
        result = _discover_provider(domain)
        _cfg["providers_discovered"][domain] = result
        if result["efa_url"] and not efa_set:
            NAHVERKEHR_URL = result["efa_url"]
            if result["efa_stop_id"]:
                STOP_ID_NAHVERKEHR = result["efa_stop_id"]
            efa_set = True
            log.info("[Discovery] Nahverkehr EFA: %s, Stop-ID: %s",
                     NAHVERKEHR_URL, STOP_ID_NAHVERKEHR)
        if result["hafas_url"] and not hafas_set:
            _HAFAS_URL = result["hafas_url"]
            hafas_set = True
            if result["hafas_auth"]:
                _cfg["hafas_auth"] = result["hafas_auth"]
        if result["scraping_url"]:
            _cfg["providers_scraping"].append({
                "domain": domain,
                "url": result["scraping_url"],
                "selector": result["scraping_selector"],
            })
    log.info("[Discovery] Ergebnis: EFA=%s, HAFAS=%s, Scraping=%d",
             NAHVERKEHR_URL if efa_set else "nein",
             _HAFAS_URL if hafas_set else "nein",
             len(_cfg["providers_scraping"]))


def _get_home_stop_ids():
    """Alle bekannten Stop-IDs fuer den Heimatbahnhof (fuer Stopover-Filterung)."""
    return {STOP_ID_DB, STOP_ID_NAHVERKEHR, "638806"}


def check_provider_availability():
    """Einmaliger Test-Request pro Provider beim Start. Setzt _provider_available."""
    global _provider_available
    # --- Nahverkehr (EFA) ---
    if NAHVERKEHR_URL:
        try:
            live_params = dict(NAHVERKEHR_PARAMS)
            live_params["name_dm"] = STOP_ID_NAHVERKEHR
            live_params["depSequence"] = 1
            r = requests.get(NAHVERKEHR_URL, params=live_params, timeout=5)
            _provider_available["nahverkehr"] = (r.status_code == 200)
        except Exception as exc:
            _provider_available["nahverkehr"] = False
            log.warning("[Startup] Nahverkehr EFA nicht erreichbar: %s", exc)
    else:
        _provider_available["nahverkehr"] = False
    # --- DB ---
    try:
        r = requests.get(API_DB, params={"results": 1, "duration": 10}, timeout=6)
        _provider_available["db"] = (r.status_code == 200)
    except Exception as exc:
        _provider_available["db"] = False
        log.warning("[Startup] DB REST nicht erreichbar: %s", exc)
    # --- S-Bahn (Scraping) ---
    scrapers = _cfg.get("providers_scraping", [])
    sbahn_url = scrapers[0].get("url", _SBAHN_URL) if scrapers else _SBAHN_URL
    try:
        r = requests.get(sbahn_url, timeout=8, headers={"User-Agent": "Mozilla/5.0 (RSS-Feed-Bot)"})
        _provider_available["sbahn"] = (r.status_code == 200)
    except Exception as exc:
        _provider_available["sbahn"] = False
        log.warning("[Startup] S-Bahn-Website nicht erreichbar: %s", exc)
    log.info("[Startup] Provider-Verfuegbarkeit: %s", _provider_available)


# ---------------------------------------------------------------------------
# Stale-Cache fuer Zwischenhalte (Gedaechtnis bei DB-Ausfall)
# ---------------------------------------------------------------------------
_stopovers_memory = {}

# ---------------------------------------------------------------------------
# S-Bahn Hannover Stoerungsmeldungen (Lauftext-Scraping)
# ---------------------------------------------------------------------------
_SBAHN_URL = "https://www.sbahn-hannover.de/"
_sbahn_cache = {"data": [], "ts": 0, "stale": []}
_SBAHN_CACHE_TTL = 300  # 5 Minuten


def _fetch_sbahn_announcements():
    """Lauftext-Meldungen vom konfigurierten S-Bahn-Provider scrapen."""
    if not _provider_available.get("sbahn", False):
        return _sbahn_cache.get("stale", [])
    now_ts = datetime.now(BERLIN_TZ).timestamp()
    if _sbahn_cache["data"] and (now_ts - _sbahn_cache["ts"]) < _SBAHN_CACHE_TTL:
        return _sbahn_cache["data"]
    scrapers = _cfg.get("providers_scraping", [])
    sbahn_url = scrapers[0].get("url", _SBAHN_URL) if scrapers else _SBAHN_URL
    selector_str = scrapers[0].get("selector", "li.main-announcements__text") if scrapers else "li.main-announcements__text"
    try:
        resp = requests.get(sbahn_url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (RSS-Feed-Bot)"
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        if "." in selector_str:
            tag_part, cls_part = selector_str.split(".", 1)
            items = soup.find_all(tag_part or True, class_=cls_part)
        else:
            items = soup.select(selector_str)
        announcements = []
        for item in items:
            text = item.get_text(strip=True)
            if text:
                announcements.append(text)
        _sbahn_cache["data"] = announcements
        _sbahn_cache["ts"] = now_ts
        if announcements:
            _sbahn_cache["stale"] = announcements
        log.info("S-Bahn Meldungen geladen: %d Stueck", len(announcements))
        return announcements
    except Exception as e:
        log.warning("S-Bahn Meldungen Fehler: %s", e)
        return _sbahn_cache.get("stale", [])

# ---------------------------------------------------------------------------
# Nahverkehr / GVH HAFAS Linienmeldungen
# Reihenfolge: S-Bahn, sprinti, 300er/500er, Stadtbahn, 100er/200er/800er, Rest
# ---------------------------------------------------------------------------
_HAFAS_URL = "https://gvh.hafas.de/hamm"
_HAFAS_LINES = [
    # S-Bahn (Priorität 1)
    "S1", "S2",
    # sprinti (Priorität 1.5)
    "SPRINTI",
    # 300er und 500er Busse (Priorität 2)
    "300-390", "500-581",
    # Stadtbahn (Priorität 3)
    "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9",
    "U10", "U11", "U12", "U13", "U17",
    # 100er, 200er, 800er Busse (Priorität 4)
    "100-170", "200-254", "800-870",
    # Alle anderen Busgruppen (Priorität 5)
    "400-492", "600-699", "700-799",
]

# Prioritäts-Mapping für Sortierung
_HAFAS_PRIORITY = {
    "S1": 1, "S2": 1,
    "SPRINTI": 1.5,
    "300-390": 2, "500-581": 2,
    "U1": 3, "U2": 3, "U3": 3, "U4": 3, "U5": 3, "U6": 3, "U7": 3,
    "U8": 3, "U9": 3, "U10": 3, "U11": 3, "U12": 3, "U13": 3, "U17": 3,
    "100-170": 4, "200-254": 4, "800-870": 4,
    "400-492": 5, "600-699": 5, "700-799": 5,
}
_nahverkehr_cache = {"data": [], "ts": 0, "stale": []}
_NAHVERKEHR_CACHE_TTL = 300  # 5 Minuten


# Keywords fuer Fahrtstoerungen vs. Infrastruktur-Meldungen
_FAHRSTOERUNG_KEYWORDS = [
    "schienenersatzverkehr", "sperrung", "ausfall", "umleitung", "fahrtausfall",
    "streckenabschnitt", "gleiswechsel", "verspaetung", "verzoegerung", "verspätung",
    "signalstoerung", "oberleitungsschaden", "oberleitungsstoerung", "technische stoerung",
    "weichendefekt", "weichenstoerung", "streik", "unwetter", "sturm", "blitzschlag",
    "bahnuebergang", "bahnübergang", "personen im gleis", "notarzteinsatz", "polizeieinsatz",
    "witterungsbedingt", "bauarbeiten", "generalsanierung", "defekt"
]
_INFRASTRUKTUR_KEYWORDS = [
    "aufzug", "rolltreppe", "fahrstuhl", "treppe", "aufzugsanlage", "rolltreppenanlage",
    "haltestelle wird verlegt", "bahnsteig verlegt", "haltestelle eingeschraenkt",
    "fahrkartenautomat", "fahrkartenschalter", "ticketautomat", "schalter",
    "beleuchtung", "beschilderung", "reinigung", "wartung", "instandhaltung",
    "barrierefreiheit", "behindertengerecht", "rollstuhl", "blinde", "sehbehinderte"
]

def _categorize_message(title, text):
    """Kategorisiere eine Meldung als Fahrstoerung (0) oder Infrastruktur (1)."""
    combined = (title + " " + text).lower()
    for kw in _FAHRSTOERUNG_KEYWORDS:
        if kw in combined:
            return 0  # Fahrstoerung
    for kw in _INFRASTRUKTUR_KEYWORDS:
        if kw in combined:
            return 1  # Infrastruktur
    return 0  # Default: Fahrstoerung

def _fetch_nahverkehr_messages():
    """Linienmeldungen von der GVH HAFAS API abrufen und nach Priorität sortieren."""
    import time as _time
    import re
    if not _provider_available.get("nahverkehr", False):
        return _nahverkehr_cache.get("stale", [])
    now_ts = datetime.now(BERLIN_TZ).timestamp()
    if _nahverkehr_cache["data"] and (now_ts - _nahverkehr_cache["ts"]) < _NAHVERKEHR_CACHE_TTL:
        return _nahverkehr_cache["data"]
    hafas_auth = _cfg.get("hafas_auth", "IKSEvZ1SsVdfIRSK")
    try:
        seen_titles = set()
        messages_with_priority = []
        for line in _HAFAS_LINES:
            payload = {
                "ver": "1.62",
                "lang": "deu",
                "auth": {"type": "AID", "aid": hafas_auth},
                "client": {"id": "HAFAS", "type": "WEB", "name": "webapp",
                           "l": "vs_webapp", "v": 10109},
                "formatted": False,
                "svcReqL": [{
                    "meth": "LineSearch",
                    "req": {"grpCtx": line, "reslvHimMsgs": True},
                    "id": "1|8|"
                }]
            }
            params = {
                "hciMethod": "LineSearch",
                "hciVersion": "1.62",
                "hciClientType": "WEB",
                "hciClientVersion": "10109",
                "aid": hafas_auth,
                "rnd": str(int(_time.time() * 1000))
            }
            resp = requests.post(_HAFAS_URL, json=payload, params=params,
                                 timeout=8, headers={
                                     "User-Agent": "Mozilla/5.0 (RSS-Feed-Bot)",
                                     "Content-Type": "application/json",
                                     "Origin": "https://gvh.hafas.de",
                                     "Referer": "https://gvh.hafas.de/"
                                 })
            resp.raise_for_status()
            data = resp.json()
            svc = data.get("svcResL", [{}])[0]
            res = svc.get("res", {})
            common = res.get("common", {})
            him_list = common.get("himL", [])
            for h in him_list:
                title = h.get("head", "")
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    # HTML-Tags aus dem Text entfernen
                    text = h.get("text", "")
                    text = text.replace("<br>", "\n").replace("<br/>", "\n")
                    text = text.replace("<br />", "\n")
                    # Restliche HTML-Tags entfernen
                    text = re.sub(r"<[^>]+>", "", text)
                    priority = _HAFAS_PRIORITY.get(line, 99)
                    # Kategorisiere die Meldung (0=Fahrstoerung, 1=Infrastruktur)
                    category = _categorize_message(title, text)
                    messages_with_priority.append({
                        "priority": priority,
                        "category": category,
                        "title": title,
                        "text": text
                    })
        # Sortiere nach Priorität (Liniengruppe), dann nach Kategorie (Fahrstoerung vor Infrastruktur)
        messages_with_priority.sort(key=lambda x: (x["priority"], x["category"]))
        messages = [{"title": m["title"], "text": m["text"], "category": m["category"]} for m in messages_with_priority]
        _nahverkehr_cache["data"] = messages
        _nahverkehr_cache["ts"] = now_ts
        if messages:
            _nahverkehr_cache["stale"] = messages
        log.info("Nahverkehr Meldungen geladen: %d Stueck", len(messages))
        return messages
    except Exception as e:
        log.warning("Nahverkehr Meldungen Fehler: %s", e)
        return _nahverkehr_cache.get("stale", [])


# Cache fuer trip_ids (Linie+Zeit+Richtung -> trip_id)
_trip_id_cache = {}

# Platzhalter fuer Pfeile (werden NACH XML-Escaping ersetzt)
_ARROW_RIGHT = "__ARROW_RIGHT__"
_ARROW_LEFT  = "__ARROW_LEFT__"

# ---------------------------------------------------------------------------
# Umlaute ersetzen (Fritz!Fon-kompatibel)
# ---------------------------------------------------------------------------
_UMLAUT_MAP = {
    "\u00e4": "ae", "\u00f6": "oe", "\u00fc": "ue", "\u00df": "ss",
    "\u00c4": "Ae", "\u00d6": "Oe", "\u00dc": "Ue",
    "\u00e9": "e", "\u00e8": "e", "\u00ea": "e",
    "\u00e0": "a", "\u00e1": "a",
    "\u00f4": "o", "\u00f2": "o",
    "\u00fb": "u", "\u00f9": "u",
}


def _sanitize(text):
    if not text:
        return ""
    for char, replacement in _UMLAUT_MAP.items():
        text = text.replace(char, replacement)
    text = text.encode("ascii", "replace").decode("ascii")
    return text


# ---------------------------------------------------------------------------
# Robuste HTTP-Session
# ---------------------------------------------------------------------------
def _build_session():
    session = requests.Session()
    retries = Retry(
        total=1,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Wennigsen-RSS-Feed/3.0 "
                      "(https://abfahrten-wennigsen-bhf.onrender.com)"
    })
    return session


http = _build_session()

# ---------------------------------------------------------------------------
# API-URLs
# ---------------------------------------------------------------------------
NAHVERKEHR_URL = "https://abfahrten.uestra.de/proxy2/efa/XML_DM_REQUEST"
NAHVERKEHR_PARAMS = {
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
    "name_dm": STOP_ID_NAHVERKEHR,
    "c": 1,
}

API_DB   = f"https://v6.db.transport.rest/stops/{STOP_ID_DB}/departures"
TRIPS_DB = "https://v6.db.transport.rest/trips"


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------
def parse_time(iso_time):
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
    return dt.strftime("%H:%M") if dt else "---"


def _extract_platform(bon_str):
    if not bon_str:
        return ""
    parts = bon_str.split(":")
    if len(parts) >= 5:
        candidate = parts[-1]
        if candidate.isdigit() and len(candidate) <= 2:
            return candidate
    return ""


def _clean_line_name(raw):
    if raw.startswith("S-Bahn "):
        raw = raw[7:]
    # "Nachtliner" Praefix entfernen (auch mehrfach, z.B. "NachtlinerNachtlinerN56" -> "N56")
    while raw.startswith("Nachtliner"):
        raw = raw[10:]
    # Leerzeichen in Liniennamen entfernen (z.B. "S 1" -> "S1")
    raw = raw.replace(" ", "")
    return raw


# ---------------------------------------------------------------------------
# Nahverkehr EFA API
# ---------------------------------------------------------------------------
def _fetch_nahverkehr():
    if not _provider_available.get("nahverkehr", False):
        return []
    live_params = dict(NAHVERKEHR_PARAMS)
    live_params["name_dm"] = STOP_ID_NAHVERKEHR
    try:
        resp = http.get(NAHVERKEHR_URL, params=live_params, timeout=5)
        if resp.status_code != 200:
            log.warning("Nahverkehr EFA Status %s", resp.status_code)
            return []
        data = resp.json()
    except Exception as e:
        log.error("Nahverkehr EFA fehlgeschlagen: %s", e)
        return []

    now = datetime.now(BERLIN_TZ)
    results = []

    for rd in data.get("departures", []):
        line_name = _clean_line_name(rd.get("line", "---"))
        number    = rd.get("number", "")
        if not line_name:
            line_name = number if number else "---"
        direction = rd.get("destination", "---")
        platform  = _extract_platform(rd.get("bon", ""))

        hints_text = []
        for h in rd.get("hints", []):
            content = h.get("content", "")
            htype   = h.get("type", "")
            if htype != "VehicleType" and content:
                hints_text.append(content)

        disruptions = []
        for info in rd.get("infos", []):
            title = info.get("title", "")
            text  = info.get("text", "")
            if title:
                disruptions.append(title)
            elif text:
                disruptions.append(text)

        for event in rd.get("events", []):
            planned_str = event.get("plannedTime")
            actual_str  = event.get("estimated_time") or planned_str
            if not planned_str:
                continue

            planned_dt = parse_time(planned_str)
            actual_dt  = parse_time(actual_str) or planned_dt

            ref_dt = actual_dt or planned_dt
            if not ref_dt or ref_dt < now:
                continue

            delay_sec = 0
            if planned_dt and actual_dt:
                delay_sec = max(0, int((actual_dt - planned_dt).total_seconds()))

            results.append({
                "line":       line_name,
                "number":     number,
                "direction":  direction,
                "planned_dt": planned_dt,
                "actual_dt":  actual_dt,
                "delay":      delay_sec,
                "platform":   platform,
                "cancelled":  False,
                "remarks":    disruptions[:],
                "hints":      hints_text[:],
                "source":     "nahverkehr",
                "trip_id":    None,
            })

    log.info("Nahverkehr EFA: %d Abfahrten geladen.", len(results))
    return results


# ---------------------------------------------------------------------------
# DB API (mit 5-Minuten-Cache um Timeouts zu vermeiden)
# ---------------------------------------------------------------------------
_db_cache = {"data": [], "trips_url": None, "ts": 0}
_DB_CACHE_TTL = 300  # 5 Minuten


def _fetch_db():
    if not _provider_available.get("db", False):
        if _db_cache["data"]:
            return _db_cache["data"], _db_cache.get("trips_url") or TRIPS_DB
        return [], TRIPS_DB
    now_ts = time.time()
    if _db_cache["data"] and (now_ts - _db_cache["ts"]) < _DB_CACHE_TTL:
        log.info("DB: Nutze Cache (%d Abfahrten, Alter: %ds).",
                 len(_db_cache["data"]), int(now_ts - _db_cache["ts"]))
        return _db_cache["data"], _db_cache["trips_url"] or TRIPS_DB

    now = datetime.now(BERLIN_TZ)
    try:
        resp = http.get(
            API_DB,
            params={"results": 30, "duration": 180,
                    "remarks": "true", "language": "de"},
            timeout=6,
        )
        if resp.status_code != 200:
            log.warning("DB Status %s", resp.status_code)
            return [], TRIPS_DB
        raw = resp.json().get("departures", [])
        if not raw:
            log.warning("DB keine Abfahrten.")
            return [], TRIPS_DB

        results = []
        for d in raw:
            cancelled    = d.get("cancelled", False)
            when_str     = d.get("when") or d.get("plannedWhen")
            planned_when = d.get("plannedWhen")

            planned_dt = parse_time(planned_when)
            actual_dt  = parse_time(when_str) if when_str else planned_dt

            if not actual_dt:
                actual_dt = planned_dt
            if not planned_dt:
                continue

            ref = actual_dt or planned_dt
            if not cancelled and ref < now:
                continue
            if cancelled and planned_dt < now:
                continue

            delay = d.get("delay") or 0
            if not isinstance(delay, (int, float)):
                delay = 0

            remarks_list = []
            for rm in d.get("remarks", []):
                rm_type = rm.get("type", "")
                rm_text = rm.get("text", "") or rm.get("summary", "")
                if rm_type in ("warning", "status") and rm_text:
                    remarks_list.append(rm_text)

            line_obj = d.get("line", {})
            results.append({
                "line":       _clean_line_name(line_obj.get("name", "---")),
                "number":     line_obj.get("productName", ""),
                "direction":  d.get("direction", "---"),
                "planned_dt": planned_dt,
                "actual_dt":  actual_dt,
                "delay":      delay,
                "platform":   d.get("platform", ""),
                "cancelled":  cancelled,
                "remarks":    remarks_list,
                "hints":      [],
                "source":     "db",
                "trip_id":    d.get("tripId"),
            })

        log.info("DB: %d Abfahrten geladen.", len(results))
        _db_cache["data"] = results
        _db_cache["trips_url"] = TRIPS_DB
        _db_cache["ts"] = time.time()
        return results, TRIPS_DB

    except Exception as e:
        log.error("DB fehlgeschlagen: %s", e)
        if _db_cache["data"]:
            log.info("DB: Nutze letzten Cache als Fallback (%d Abfahrten).", len(_db_cache["data"]))
            return _db_cache["data"], _db_cache["trips_url"] or TRIPS_DB
        return [], TRIPS_DB


# ---------------------------------------------------------------------------
# Zwischenhalte laden (mit Gedaechtnis-Fallback bei DB-Ausfall)
# ---------------------------------------------------------------------------
def _fetch_stopovers(trip_id, trips_url):
    """Zwischenhalte laden. Bei Fehler wird aus dem Gedaechtnis geladen.
    Gibt ein 3-Tupel zurueck: (stops, trip_remarks, is_stale)
    is_stale=True bedeutet: Daten stammen aus dem Gedaechtnis.
    """
    global _stopovers_memory

    if not trip_id or not trips_url:
        cached = _stopovers_memory.get(trip_id)
        if cached:
            return (cached[0], cached[1], True)
        return ((), (), False)

    try:
        encoded = quote(trip_id, safe="")
        resp = http.get(
            f"{trips_url}/{encoded}",
            params={"stopovers": "true", "remarks": "true", "language": "de"},
            timeout=6,
        )
        if resp.status_code != 200:
            log.warning("Stopovers Status %s fuer Trip %s - nutze Gedaechtnis",
                        resp.status_code, trip_id[:40])
            cached = _stopovers_memory.get(trip_id)
            if cached:
                return (cached[0], cached[1], True)
            return ((), (), False)

        trip_data    = resp.json().get("trip", {})
        raw          = trip_data.get("stopovers", [])
        trip_remarks = []

        for rm in trip_data.get("remarks", []):
            rm_type = rm.get("type", "")
            rm_text = rm.get("text", "") or rm.get("summary", "")
            if rm_type in ("warning", "status") and rm_text:
                trip_remarks.append(rm_text)

        found = False
        stops = []
        for s in raw:
            sid        = str(s.get("stop", {}).get("id", ""))
            station_id = str(s.get("stop", {}).get("station", {}).get("id", ""))
            _home_ids = _get_home_stop_ids()
            if sid in _home_ids or station_id in _home_ids:
                found = True
                continue
            if found:
                sname       = s.get("stop", {}).get("name", "---")
                arr_actual  = parse_time(s.get("arrival") or s.get("plannedArrival"))
                arr_planned = parse_time(s.get("plannedArrival"))
                s_cancelled = s.get("cancelled", False)

                s_delay = 0
                if arr_actual and arr_planned:
                    s_delay = max(0, int((arr_actual - arr_planned).total_seconds()) // 60)

                stops.append((fmt(arr_actual), sname, s_cancelled, s_delay))

        result_stops   = tuple(stops)
        result_remarks = tuple(trip_remarks)

        # Im Gedaechtnis speichern (max 200 Eintraege)
        _stopovers_memory[trip_id] = (result_stops, result_remarks)
        if len(_stopovers_memory) > 200:
            oldest_key = next(iter(_stopovers_memory))
            del _stopovers_memory[oldest_key]

        return (result_stops, result_remarks, False)

    except Exception as e:
        log.error("Stopovers-Fehler: %s - nutze Gedaechtnis", e)
        cached = _stopovers_memory.get(trip_id)
        if cached:
            return (cached[0], cached[1], True)
        return ((), (), False)


# ---------------------------------------------------------------------------
# Hauptlogik: Abfahrten zusammenfuehren
# ---------------------------------------------------------------------------
def _get_departures():
    nahverkehr_deps        = _fetch_nahverkehr()
    db_deps, trips_url = _fetch_db()

    if nahverkehr_deps:
        # Primaeres Lookup: Linie + Richtung (normalisiert) + Zeit
        db_lookup = {}
        # Sekundaeres Lookup: nur Linie + Zeit (fuer unterschiedliche Richtungsnamen)
        db_lookup_no_dir = {}
        for d in db_deps:
            if d["planned_dt"]:
                norm_line = d["line"].replace(" ", "")
                # Richtung normalisieren (nur erste 10 Zeichen, kleingeschrieben)
                norm_dir = d["direction"][:10].lower()
                key = (norm_line, norm_dir, d["planned_dt"].strftime("%H:%M"))
                db_lookup[key] = d
                lt_key = (norm_line, d["planned_dt"].strftime("%H:%M"))
                db_lookup_no_dir.setdefault(lt_key, d)

        enriched = []
        for dep in nahverkehr_deps:
            if not dep["planned_dt"]:
                enriched.append(dep)
                continue

            norm_line = dep["line"].replace(" ", "")
            norm_dir = dep["direction"][:10].lower()
            base_key = (norm_line, norm_dir, dep["planned_dt"].strftime("%H:%M"))
            db_match = db_lookup.get(base_key)

            if not db_match:
                for offset_min in [-2, -1, 1, 2]:
                    alt_time = dep["planned_dt"] + timedelta(minutes=offset_min)
                    alt_key  = (norm_line, norm_dir, alt_time.strftime("%H:%M"))
                    db_match = db_lookup.get(alt_key)
                    if db_match:
                        break

            # Fallback: nur Linie + Zeit (ignoriere Richtung)
            if not db_match:
                lt_key = (norm_line, dep["planned_dt"].strftime("%H:%M"))
                db_match = db_lookup_no_dir.get(lt_key)
                if not db_match:
                    for offset_min in [-2, -1, 1, 2]:
                        alt_time = dep["planned_dt"] + timedelta(minutes=offset_min)
                        lt_key2 = (norm_line, alt_time.strftime("%H:%M"))
                        db_match = db_lookup_no_dir.get(lt_key2)
                        if db_match:
                            break

            if db_match:
                dep["trip_id"] = db_match.get("trip_id")
                if not dep["platform"] and db_match.get("platform"):
                    dep["platform"] = db_match["platform"]
                if db_match.get("cancelled"):
                    dep["cancelled"] = True
                if db_match.get("remarks"):
                    dep["remarks"] = list(set(dep["remarks"] + db_match["remarks"]))
                # trip_id im Cache speichern
                if db_match.get("trip_id"):
                    cache_key = (norm_line, dep["planned_dt"].strftime("%H:%M"))
                    _trip_id_cache[cache_key] = db_match["trip_id"]
                    log.info("trip_id gespeichert fuer %s %s", norm_line, dep["planned_dt"].strftime("%H:%M"))
            else:
                # Fallback: trip_id aus Cache laden
                cache_key = (norm_line, dep["planned_dt"].strftime("%H:%M"))
                cached_trip = _trip_id_cache.get(cache_key)
                if cached_trip:
                    dep["trip_id"] = cached_trip
                    log.info("trip_id aus Cache fuer %s %s", norm_line, dep["planned_dt"].strftime("%H:%M"))
                else:
                    log.warning("Keine trip_id fuer %s %s (weder DB noch Cache)", norm_line, dep["planned_dt"].strftime("%H:%M"))

            enriched.append(dep)

        # Duplikate vermeiden (Linie + Zeit + Richtung)
        seen_keys = set()
        unique_enriched = []
        for dep in enriched:
            if dep["planned_dt"]:
                # Normalisierter Key fuer Duplikats-Check
                norm_line = dep["line"].replace(" ", "")
                # Nur die ersten 10 Zeichen der Richtung fuer robustes Matching
                norm_dir = dep["direction"][:10].lower()
                key = (norm_line, norm_dir, dep["planned_dt"].strftime("%H:%M"))
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_enriched.append(dep)
        
        # Abgesagte Züge von DB hinzufügen, falls noch nicht vorhanden
        for d in db_deps:
            if d.get("cancelled") and d["planned_dt"]:
                norm_line = d["line"].replace(" ", "")
                norm_dir = d["direction"][:10].lower()
                key = (norm_line, norm_dir, d["planned_dt"].strftime("%H:%M"))
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_enriched.append(d)

        final       = unique_enriched
        source_info = "Nahverkehr + DB"
    elif db_deps:
        final       = db_deps
        source_info = "DB (Fallback)"
    else:
        final       = []
        source_info = "Keine Daten"

    _max_dt = datetime.max.replace(tzinfo=pytz.utc)
    final_sorted = sorted(
        final,
        key=lambda x: (
            x.get("actual_dt") or x.get("planned_dt") or _max_dt,
            1 if x.get("cancelled") else 0,
        ),
    )[:MAX_DEPARTURES]

    log.info("Feed: %s (%d Abfahrten)", source_info, len(final_sorted))
    return (tuple(final_sorted), trips_url, source_info)


# ---------------------------------------------------------------------------
# RSS-Feed generieren (Fritz!Fon-kompatibel)
# ---------------------------------------------------------------------------
def _build_feed():
    deps_tuple, trips_url, source_info = _get_departures()
    now     = datetime.now(BERLIN_TZ)
    now_str = now.strftime("%a, %d %b %Y %H:%M:%S %z")

    departures = []
    for dep in deps_tuple:
        ref_dt = dep.get("actual_dt") or dep.get("planned_dt")
        if dep.get("cancelled"):
            if dep.get("planned_dt") and dep["planned_dt"] < now - timedelta(minutes=5):
                continue
        elif ref_dt and ref_dt < now - timedelta(minutes=5):
            # Entferne Abfahrten, die mehr als 5 Minuten in der Vergangenheit liegen
            continue
        departures.append(dep)

    lines = []
    lines.append('<?xml version="1.0" encoding="ISO-8859-1"?>')
    lines.append('<rss version="2.0">')
    lines.append('<channel>')
    lines.append(f'<title>Abfahrten {_sanitize(STOP_NAME)}</title>')
    lines.append('<link>https://www.gvh.de</link>')
    lines.append(f'<description>Naechste Abfahrten am {_sanitize(STOP_NAME)}</description>')
    lines.append('<language>de-de</language>')
    lines.append('<ttl>1</ttl>')
    lines.append(f'<lastBuildDate>{now_str}</lastBuildDate>')

    # --- S-Bahn Hannover Lauftext-Meldungen ---
    sbahn_announcements = _fetch_sbahn_announcements()
    if sbahn_announcements:
        lines.append('<item>')
        if len(sbahn_announcements) == 1:
            lines.append('<title>!!! S-Bahn Meldung !!!</title>')
        else:
            lines.append(f'<title>!!! {len(sbahn_announcements)} S-Bahn Meldungen !!!</title>')
        desc_parts = []
        for ann in sbahn_announcements:
            desc_parts.append(_sanitize(ann))
            desc_parts.append("")
        desc_text = "\n".join(desc_parts).strip()
        lines.append(f'<description><![CDATA[{desc_text}]]></description>')
        lines.append('</item>')

    # --- GROSSSTOERUNGEN erkennen ---
    # Schluesselwoerter fuer Grossstoerungen (Streik, Unwetter etc.)
    _DISRUPTION_KEYWORDS = [
        "streik", "warnstreik", "arbeitskampf",
        "unwetter", "sturm", "hochwasser", "orkan",
        "oberleitungsschaden", "oberleitungsstoerung", "oberleitung",
        "stellwerkstoerung", "stellwerksstoerung", "signalstoerung",
        "sperrung", "gesperrt", "streckensperrung",
        "schienenersatzverkehr", "sev",
        "notarzteinsatz", "polizeieinsatz",
        "personen im gleis", "personenunfall",
        "bombenentschaerfung", "bombenfund",
        "gleisstoerung", "weichenstoerung",
        "zugausfall", "totalausfall",
        "eingeschraenkt", "massiv",
    ]

    # Stoerungen aus allen Abfahrten sammeln
    disruption_map = {}  # text -> {lines: set, count: int}
    for dep in departures:
        dep_line = dep.get("line", "---")
        for rm in dep.get("remarks", []):
            rm_lower = _sanitize(rm).lower()
            is_disruption = any(kw in rm_lower for kw in _DISRUPTION_KEYWORDS)
            if is_disruption:
                rm_clean = _sanitize(rm)
                if rm_clean not in disruption_map:
                    disruption_map[rm_clean] = {"lines": set(), "count": 0}
                disruption_map[rm_clean]["lines"].add(dep_line)
                disruption_map[rm_clean]["count"] += 1

    # Grossstoerungen als ERSTEN Eintrag anzeigen
    if disruption_map:
        lines.append('<item>')
        # Titel: Anzahl Stoerungen
        n = len(disruption_map)
        if n == 1:
            lines.append('<title>*** STOERUNG ***</title>')
        else:
            lines.append(f'<title>*** {n} STOERUNGEN ***</title>')

        # Details
        d_parts = []
        for rm_text, info in disruption_map.items():
            affected = ", ".join(sorted(info["lines"]))
            d_parts.append(f"{rm_text}")
            d_parts.append(f"Betrifft: {affected}")
            d_parts.append("")

        d_text = "\n".join(d_parts)
        lines.append(f'<description><![CDATA[{d_text}]]></description>')
        lines.append('</item>')

    if not departures:
        lines.append('<item>')
        lines.append('<title>Keine Abfahrten verfuegbar</title>')
        lines.append('<description><![CDATA[Alle Datenquellen liefern derzeit keine Abfahrten.]]></description>')
        lines.append('</item>')
    else:
        for dep in departures:
            line       = _clean_line_name(dep.get("line", "---"))
            line = re.sub(r'Nacht\w*', '', line).strip()
            if not line:
                line = dep.get("number", "") or "---"
            # Leerzeichen nach 'Bus' einfuegen falls fehlend (z.B. "Bus580" -> "Bus 580")
            if line.startswith("Bus") and len(line) > 3 and line[3].isdigit():
                line = "Bus " + line[3:]
            direction  = _sanitize(dep.get("direction", "---"))
            platform   = dep.get("platform", "")
            cancelled  = dep.get("cancelled", False)
            delay      = dep.get("delay", 0)
            planned_dt = dep.get("planned_dt")
            actual_dt  = dep.get("actual_dt")

            direction_short = direction.replace("Hauptbahnhof", "Hbf.")
            direction_short = direction_short.replace("Bahnhof", "Bhf.")
            direction_short = _sanitize(direction_short)

            is_train = line.upper().startswith("S") and any(c.isdigit() for c in line)
            arrow = ""
            
            # Details sammeln
            remarks        = dep.get("remarks", [])
            trip_id        = dep.get("trip_id")
            trip_remarks   = []
            stopover_lines = []
            next_stop_name = ""

            if trip_id and trips_url:
                log.info("Lade Stopovers fuer %s (trip_id: %s...)", line, trip_id[:20])
                so_result = _fetch_stopovers(trip_id, trips_url)
                if so_result and len(so_result) == 3:
                    stops, t_remarks, is_stale = so_result
                    trip_remarks = list(t_remarks)
                    
                    # Naechsten Halt fuer Pfeil-Logik bestimmen
                    if stops:
                        next_stop_name = stops[0][1].lower()
                    
                    if is_stale and stops:
                        stopover_lines.append("[offline] Halte aus Gedaechtnis:")
                    for s_time, s_name, s_cancelled, s_delay in stops[:MAX_STOPS]:
                        s_name_clean = _sanitize(s_name)
                        # Jeder Halt bekommt eine eigene Zeile (wird spaeter mit \n zusammengefuegt)
                        if is_stale:
                            if s_cancelled:
                                stopover_lines.append(f"~~ | {s_name_clean} [entfaellt]")
                            else:
                                stopover_lines.append(f"~~ | {s_name_clean}")
                        else:
                            delay_part   = f" (+{s_delay})" if s_delay > 0 else ""
                            if s_cancelled:
                                stopover_lines.append(f"{s_time}{delay_part} | {s_name_clean} [entfaellt]")
                            else:
                                stopover_lines.append(f"{s_time}{delay_part} | {s_name_clean}")
                    if len(stops) > MAX_STOPS:
                        stopover_lines.append("... weitere Halte")

            # Pfeil-Logik basierend auf naechstem Halt und Config-Richtungszielen
            if is_train:
                _cfg_right = _cfg.get("direction_right", "hannover").lower()
                _cfg_left  = _cfg.get("direction_left",  "barsinghausen").lower()
                _next3 = [s[1].lower() for s in (stops[:3] if trip_id and trips_url else [])]
                if any(_cfg_right in s or s in _cfg_right for s in _next3):
                    arrow = _ARROW_RIGHT
                elif any(_cfg_left in s or s in _cfg_left for s in _next3):
                    arrow = _ARROW_LEFT
                elif "lemmie" in next_stop_name:
                    arrow = _ARROW_RIGHT
                elif "egestorf" in next_stop_name:
                    arrow = _ARROW_LEFT
                else:
                    # Fallback auf Richtungs-Config falls naechster Halt unbekannt
                    dir_lower = direction.lower()
                    dir_right_kw = _cfg.get("direction_right", "hannover").lower()
                    dir_left_kw  = _cfg.get("direction_left",  "barsinghausen").lower()
                    _right_stations = ["hannover", "hbf", "hauptbahnhof", "seelze", "nienburg", "minden", "wunstorf", "celle", dir_right_kw]
                    _left_stations  = ["haste", "egestorf", "barsinghausen", dir_left_kw]
                    if any(st in dir_lower for st in _right_stations):
                        arrow = _ARROW_RIGHT
                    elif any(st in dir_lower for st in _left_stations):
                        arrow = _ARROW_LEFT
                    else:
                        arrow = "-"

            time_str  = fmt(actual_dt or planned_dt)
            delay_str = ""
            if cancelled:
                time_str = f"[AUSFALL] {fmt(planned_dt)}"
            elif delay >= 60:
                delay_min = delay // 60
                delay_str = f" (+{delay_min})"

            platform_str  = f"Gl.{platform}" if platform else ""
            platform_part = f" ({platform_str})" if platform_str else ""

            if is_train:
                # Pfeile direkt einsetzen (ohne CDATA, da Fritz!Fon das im Titel oft nicht mag)
                arrow_char = ">" if arrow == _ARROW_RIGHT else ("<" if arrow == _ARROW_LEFT else "-")
                # Titel-Format: Zeit | Linie (Gleis) Pfeil Ziel
                # WICHTIG: Kein schliessendes > am Ende!
                title = f"{time_str}{delay_str} | {line}{platform_part} {arrow_char} {direction_short}"
            else:
                # Bus-Format: Zeit | Linie - Ziel
                title = f"{time_str}{delay_str} | {line} - {direction_short}"

            desc_parts = []

            if cancelled:
                desc_parts.append("*** Fahrt faellt aus ***")
                if planned_dt:
                    desc_parts.append(f"Geplant: {fmt(planned_dt)}")
            elif delay >= 60:
                delay_min = delay // 60
                desc_parts.append(
                    f"+{delay_min} Min (plan: {fmt(planned_dt)}, neu: {fmt(actual_dt)})"
                )

            seen = set()
            all_remarks = []
            for rm in remarks + trip_remarks:
                key = rm.strip()
                if key not in seen:
                    seen.add(key)
                    all_remarks.append(rm)

            if all_remarks:
                m, d  = now.month, now.day
                year  = now.year
                today = now.date()

                # Gaussscher Osteralgorithmus
                a = year % 19
                b = year // 100
                c = year % 100
                dd = b // 4
                e = b % 4
                f = (b + 8) // 25
                g = (b - f + 1) // 3
                h = (19 * a + b - dd - g + 15) % 30
                i = c // 4
                k = c % 4
                l = (32 + 2 * e + 2 * i - h - k) % 7
                mm = (a + 11 * h + 22 * l) // 451
                month_e = (h + l - 7 * mm + 114) // 31
                day_e = ((h + l - 7 * mm + 114) % 31) + 1
                
                easter_sunday = date(year, month_e, day_e)
                karfreitag = easter_sunday - timedelta(days=2)
                ostermontag = easter_sunday + timedelta(days=1)
                himmelfahrt = easter_sunday + timedelta(days=39) # Vatertag

                is_april_fools = (m == 4 and d == 1)
                is_halloween   = (m == 10 and d == 31)
                is_christmas   = (m == 12 and d in [24, 25, 26])
                is_new_year    = (m == 12 and d == 31) or (m == 1 and d == 1)
                is_star_wars   = (m == 5 and d == 4)
                is_vatertag    = (today == himmelfahrt)
                is_easter      = (karfreitag <= today <= ostermontag)

                for rm in all_remarks:
                    rm_clean = _sanitize(rm)
                    rm_lower = rm_clean.lower()

                    ignore_keywords = ["aufzug", "lift", "rolltreppe", "wc ", "toilette", "gebaeudeschliessung"]
                    if any(kw in rm_lower for kw in ignore_keywords):
                        continue

                    special_msg = None

                    if is_april_fools:
                        fools_map = {
                            "personalmangel": "Lokfuehrer hat verschlafen (Kissen war zu weich)",
                            "signalstoerung": "Signal zeigt nur noch Pink (Modetrend)",
                            "personen im gleis": "Entenfamilie uebt fuer den Ententanz",
                            "notarzteinsatz": "Einhorn-Sichtung auf den Gleisen",
                            "weichendefekt": "Weiche hat sich fuer den Urlaub entschieden",
                            "oberleitungsstoerung": "Vogel hat die Leitung als Schaukel benutzt",
                            "technische stoerung": "Der Zug hat heute einfach keine Lust",
                            "verspaetung aus vorheriger fahrt": "Zug musste noch kurz bei Oma vorbei",
                            "polizeieinsatz": "Polizei sucht nach dem verlorenen Witz",
                            "witterungsbedingt": "Schneeflocken haben eine Sitzblockade gemacht",
                            "bauarbeiten": "Gleise werden heute frisch gebuegelt",
                            "unwetter": "Wolken haben heute schlechte Laune",
                            "streik": "Zuege machen heute Yoga-Pause",
                            "defekt": "Der Zug braucht erst mal einen Kaffee",
                        }
                        for k2, v2 in fools_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Der Zug macht gerade ein Nickerchen"

                    elif is_halloween:
                        halloween_map = {
                            "personalmangel": "Lokfuehrer wurde von Geistern entfuehrt",
                            "signalstoerung": "Signale leuchten heute wie Kuerbisse",
                            "personen im gleis": "Zombies auf den Schienen gesichtet",
                            "notarzteinsatz": "Vampir-Attacke im Speisewagen",
                            "weichendefekt": "Die Weiche ist verhext",
                            "oberleitungsstoerung": "Hexenbesen in der Leitung verfangen",
                            "technische stoerung": "Spuk im Maschinenraum",
                            "verspaetung aus vorheriger fahrt": "Zug ist im Nebel des Grauens verschollen",
                            "polizeieinsatz": "Geisterjaeger im Einsatz",
                            "witterungsbedingt": "Gruseliger Nebel verlangsamt die Fahrt",
                            "bauarbeiten": "Grabungsarbeiten fuer die Unterwelt",
                            "unwetter": "Ein schreckliches Gewitter zieht auf",
                            "streik": "Skelette machen heute Pause",
                            "defekt": "Der Zug ist heute verflucht",
                        }
                        for k2, v2 in halloween_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Suesses oder Saures! Der Zug ist heute gruselig langsam"

                    elif is_christmas:
                        xmas_map = {
                            "personalmangel": "Lokfuehrer hilft dem Weihnachtsmann beim Packen",
                            "signalstoerung": "Signale leuchten heute wie Weihnachtssterne",
                            "personen im gleis": "Rentier-Herde auf den Gleisen",
                            "notarzteinsatz": "Plaetzchen-Ueberdosis im Bordbistro",
                            "weichendefekt": "Die Weiche ist eingefroren wie am Nordpol",
                            "oberleitungsstoerung": "Lichterkette in der Leitung verfangen",
                            "technische stoerung": "Wichtel in der Elektronik",
                            "verspaetung aus vorheriger fahrt": "Zug musste noch Geschenke ausliefern",
                            "polizeieinsatz": "Polizei sucht nach dem Grinch",
                            "witterungsbedingt": "Schneegestoeber wie im Wintermaerchen",
                            "bauarbeiten": "Wichtelwerkstatt auf den Gleisen",
                            "unwetter": "Rentierschlitten hat Vorfahrt",
                            "streik": "Zuege machen heute Bescherung",
                            "defekt": "Der Zug braucht eine Portion Gluehwein",
                        }
                        for k2, v2 in xmas_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Frohe Weihnachten! Der Zug geniesst die Feiertage"

                    elif is_new_year:
                        ny_map = {
                            "personalmangel": "Lokfuehrer sucht noch nach seinen Vorsaetzen",
                            "signalstoerung": "Signale funkeln wie Feuerwerk",
                            "personen im gleis": "Gluecksschweinchen auf den Gleisen",
                            "notarzteinsatz": "Zu viel Kinderpunsch getrunken",
                            "weichendefekt": "Die Weiche rutscht ins neue Jahr",
                            "oberleitungsstoerung": "Konfetti in der Leitung",
                            "technische stoerung": "System-Update fuer das neue Jahr",
                            "verspaetung aus vorheriger fahrt": "Zug hat zu lange gefeiert",
                            "polizeieinsatz": "Polizei wuenscht ein frohes neues Jahr",
                            "witterungsbedingt": "Feuerwerksnebel behindert die Sicht",
                            "bauarbeiten": "Gleise werden fuer das neue Jahr poliert",
                            "unwetter": "Gluecksregen zieht auf",
                            "streik": "Zuege machen Neujahrspause",
                            "defekt": "Der Zug hat einen Kater",
                        }
                        for k2, v2 in ny_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Guten Rutsch! Der Zug gleitet ins neue Jahr"

                    elif is_easter:
                        easter_map = {
                            "personalmangel": "Lokfuehrer sucht noch Ostereier",
                            "signalstoerung": "Signale sind heute bunt bemalt",
                            "personen im gleis": "Osterhase auf den Gleisen gesichtet",
                            "notarzteinsatz": "Zu viele Schokoeier gegessen",
                            "weichendefekt": "Die Weiche ist im Osternest versteckt",
                            "oberleitungsstoerung": "Ostereier in der Leitung verfangen",
                            "technische stoerung": "Osterkueken in der Elektronik",
                            "verspaetung aus vorheriger fahrt": "Zug musste noch Eier verstecken",
                            "polizeieinsatz": "Polizei sucht nach dem goldenen Ei",
                            "witterungsbedingt": "Aprilwetter macht was es will",
                            "bauarbeiten": "Osterhasen-Werkstatt auf den Gleisen",
                            "unwetter": "Eierregen zieht auf",
                            "streik": "Zuege machen heute Eiersuche",
                            "defekt": "Der Zug braucht eine Portion Karotten",
                        }
                        for k2, v2 in easter_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Frohe Ostern! Der Zug hoppelt heute etwas langsamer"

                    elif is_star_wars:
                        sw_map = {
                            "personalmangel": "Der Lokfuehrer ist auf die dunkle Seite gewechselt",
                            "signalstoerung": "Stoerung im Hyperraum-Antrieb",
                            "personen im gleis": "Ewoks auf den Gleisen gesichtet",
                            "notarzteinsatz": "Jedi-Ritter braucht eine Meditationspause",
                            "weichendefekt": "Die Macht ist nicht stark in dieser Weiche",
                            "oberleitungsstoerung": "Imperiale Stoersender in der Leitung",
                            "technische stoerung": "R2-D2 hat einen Kurzschluss",
                            "verspaetung aus vorheriger fahrt": "Zug musste den Kessel-Run in unter 12 Parsec schaffen",
                            "polizeieinsatz": "Sturmtruppler suchen nach diesen Droiden",
                            "witterungsbedingt": "Sandsturm auf Tatooine behindert die Sicht",
                            "bauarbeiten": "Todesstern-Konstruktion auf den Gleisen",
                            "unwetter": "Ionensturm im Anmarsch",
                            "streik": "Die Rebellen-Allianz macht heute Pause",
                            "defekt": "Der Millennium Falke... ich meine der Zug ist kaputt",
                        }
                        for k2, v2 in sw_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "May the 4th be with you! Moege die Puenktlichkeit mit uns sein"

                    elif is_vatertag:
                        vater_map = {
                            "personalmangel": "Lokfuehrer ist mit dem Bollerwagen unterwegs",
                            "signalstoerung": "Signal ist im Biergarten haengengeblieben",
                            "personen im gleis": "Vatertags-Tour blockiert die Schienen",
                            "notarzteinsatz": "Zu viel Hopfenkaltschale genossen",
                            "weichendefekt": "Die Weiche macht heute eine Herrentour",
                            "oberleitungsstoerung": "Grillwurst in der Leitung verfangen",
                            "technische stoerung": "Zapfanlage im Bordbistro klemmt",
                            "verspaetung aus vorheriger fahrt": "Zug musste noch kurz am Stammtisch halten",
                            "polizeieinsatz": "Polizei sucht nach dem verlorenen Grillmeister",
                            "witterungsbedingt": "Perfektes Grillwetter verzoegert die Abfahrt",
                            "bauarbeiten": "Gleise werden heute als Kegelbahn genutzt",
                            "unwetter": "Bierregen zieht auf",
                            "streik": "Zuege machen heute Maennerabend",
                            "defekt": "Der Zug braucht erst mal ein kuehles Blondes",
                        }
                        for k2, v2 in vater_map.items():
                            if k2 in rm_lower: special_msg = v2; break
                        if not special_msg: special_msg = "Alles Gute zum Vatertag! Der Zug rollt gemuetlich"

                    if special_msg:
                        desc_parts.append(f"Grund: {special_msg} ({rm_clean})")
                    else:
                        desc_parts.append(f"Grund: {rm_clean}")

            if stopover_lines:
                if desc_parts:
                    desc_parts.append("")
                desc_parts.append("Halte:")
                desc_parts.extend(stopover_lines)

            hints = dep.get("hints", [])
            if hints:
                if desc_parts:
                    desc_parts.append("")
                for h in hints:
                    # Allgemeine Infos wie Fahrradmitnahme etc.
                    h_clean = _sanitize(h)
                    # Redundante "Linie S1: " Praefixe entfernen
                    if ": " in h_clean:
                        h_clean = h_clean.split(": ", 1)[1]
                    desc_parts.append(f"Info: {h_clean}")

            if not desc_parts:
                desc_parts.append("Keine weiteren Infos")

            # Zeilenumbrueche in CDATA werden vom Fritz!Fon als neue Zeilen interpretiert
            desc_text = "\n".join(desc_parts)

            lines.append('<item>')
            # Titel OHNE CDATA, aber mit XML-Escaping fuer Sicherheit
            # Wir escapen nur &, < und >.
            safe_title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f'<title>{safe_title}</title>')
            lines.append(f'<description><![CDATA[{desc_text}]]></description>')
            if trip_id:
                lines.append(f'<!-- trip_id: {trip_id} -->')
            lines.append('</item>')

    # --- Nahverkehr / GVH Linienmeldungen als LETZTES Item ---
    nahverkehr_messages = _fetch_nahverkehr_messages()
    if nahverkehr_messages:
        lines.append('<item>')
        # Trenne Fahrstoerungen (category=0) und Infrastruktur (category=1)
        fahrstoerungen = [m for m in nahverkehr_messages if m.get("category", 0) == 0]
        infrastruktur = [m for m in nahverkehr_messages if m.get("category", 0) == 1]
        n_msg = len(fahrstoerungen) + len(infrastruktur)
        if n_msg == 1:
            lines.append('<title>--- Aktuelle Meldung ---</title>')
        else:
            lines.append(f'<title>--- {n_msg} Aktuelle Meldungen ---</title>')
        msg_parts = []
        current_priority = None
        priority_labels = {
            1: "=== S-BAHN ===",
            1.5: "=== SPRINTI ===",
            2: "=== 300ER UND 500ER BUSSE ===",
            3: "=== STADTBAHNEN ===",
            4: "=== 100ER, 200ER UND 800ER BUSSE ===",
            5: "=== SONSTIGE BUSSE ===",
        }
        # Zeige zuerst Fahrstoerungen
        for msg in fahrstoerungen:
            msg_priority = None
            for line, priority in _HAFAS_PRIORITY.items():
                if line in msg["title"]:
                    msg_priority = priority
                    break
            if msg_priority is None:
                msg_priority = 99
            if msg_priority != current_priority and msg_priority in priority_labels:
                if msg_parts:
                    msg_parts.append("")
                msg_parts.append(priority_labels[msg_priority])
                current_priority = msg_priority
            msg_parts.append(_sanitize(msg["title"]))
            text_lines = msg["text"].strip().split("\n")
            if text_lines:
                msg_parts.append(_sanitize(text_lines[0]))
            msg_parts.append("")
        # Zeige dann Infrastruktur-Meldungen (falls vorhanden)
        if infrastruktur:
            if msg_parts:
                msg_parts.append("")
            msg_parts.append("=== INFRASTRUKTUR-MELDUNGEN ===")
            for msg in infrastruktur:
                msg_parts.append(_sanitize(msg["title"]))
                text_lines = msg["text"].strip().split("\n")
                if text_lines:
                    msg_parts.append(_sanitize(text_lines[0]))
                msg_parts.append("")
        msg_text = "\n".join(msg_parts).strip()
        lines.append(f'<description><![CDATA[{msg_text}]]></description>')
        lines.append('</item>')

    lines.append('</channel>')
    lines.append('</rss>')

    return "\n".join(lines).encode("iso-8859-1", errors="replace")


# ---------------------------------------------------------------------------
# Hintergrund-Refresh: Feed alle 10 Minuten proaktiv aktualisieren
# ---------------------------------------------------------------------------
_feed_cache = {"xml": None, "ts": 0}
_FEED_REFRESH_INTERVAL = 120  # 2 Minuten


def _refresh_feed_background():
    """Hintergrund-Thread: Baut den Feed alle 10 Minuten neu auf."""
    while True:
        time.sleep(_FEED_REFRESH_INTERVAL)
        try:
            log.info("[Hintergrund] Starte Feed-Aktualisierung...")
            xml_bytes = _build_feed()
            _feed_cache["xml"] = xml_bytes
            _feed_cache["ts"] = time.time()
            log.info("[Hintergrund] Feed erfolgreich aktualisiert.")
        except Exception as e:
            log.error("[Hintergrund] Fehler beim Aktualisieren des Feeds: %s", e)


# ---------------------------------------------------------------------------
# Start-Sequenz: Config -> IDs -> Provider-Discovery -> Verfuegbarkeit -> Feed
# ---------------------------------------------------------------------------
load_config("config.txt")
resolve_stop_ids()
discover_providers(_cfg.get("local_providers", []))
check_provider_availability()

log.info("[Startup] Lade Feed-Cache vor...")
try:
    _feed_cache["xml"] = _build_feed()
    _feed_cache["ts"] = time.time()
    log.info("[Startup] Feed-Cache erfolgreich vorgeladen.")
except Exception as e:
    log.error("[Startup] Fehler beim Vorladen des Feed-Cache: %s", e)

_refresh_thread = threading.Thread(target=_refresh_feed_background, daemon=True)
_refresh_thread.start()

# ---------------------------------------------------------------------------
# Flask-Routen
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    stop = _sanitize(STOP_NAME)
    return (
        f"<h1>RSS-Feed {stop}</h1>"
        "<p><a href='/feed.rss'>Zum RSS-Feed</a></p>"
        "<p>Datenquellen: Nahverkehr EFA (Echtzeit) + Deutsche Bahn "
        "(Zwischenhalte &amp; Stoerungen)</p>"
        "<p>Optimiert fuer Fritz!Fon (ISO-8859-1)</p>"
    )


@app.route("/feed.rss")
@app.route("/feed")
def rss_feed():
    # Gecachten Feed sofort zurueckgeben (wird alle 10 Min. im Hintergrund aktualisiert)
    if _feed_cache["xml"] is not None:
        xml_bytes = _feed_cache["xml"]
    else:
        # Sollte nach synchronem Startup-Vorladen nicht vorkommen - Platzhalter zurueckgeben
        log.warning("Feed-Cache leer - sende Platzhalter")
        _stop = _sanitize(STOP_NAME)
        placeholder = (
            '<?xml version="1.0" encoding="iso-8859-1"?>'
            '<rss version="2.0"><channel>'
            f'<title>{_stop}</title>'
            '<link>https://abfahrten-wennigsen-bhf.onrender.com</link>'
            f'<description>Abfahrten {_stop}</description>'
            '<item><title>Daten werden geladen...</title>'
            '<description><![CDATA[Bitte in Kuerze erneut versuchen.]]></description></item>'
            '</channel></rss>'
        )
        xml_bytes = placeholder.encode("iso-8859-1")
    return Response(
        xml_bytes,
        mimetype="application/rss+xml",
        headers={
            "Content-Type": "application/rss+xml; charset=iso-8859-1",
            "Cache-Control": "no-cache, no-store, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )


@app.route("/health")
def health():
    """Leichtgewichtiger Health-Check - antwortet sofort ohne API-Aufrufe."""
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
