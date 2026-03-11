# 🚋 Wennigsen (Deister) Bahnhof RSS-Feed

Optimierter RSS-Feed für **Fritz!Fon** mit Echtzeit-Abfahrten vom Bahnhof Wennigsen (Region Hannover).

## Live-URL
```
https://abfahrten-wennigsen-bhf.onrender.com/feed
```

## Features
- ✅ **Echtzeit-Daten**: ÜSTRA-API für präzise Abfahrtszeiten und Gleisangaben.
- ✅ **Zusatzinfos**: DB-API für Zwischenhalte, Störungen und detaillierte Zug-Informationen.
- ✅ **Stale-Cache**: Fallback-Gedächtnis für Zwischenhalte bei API-Ausfällen (gekennzeichnet mit `[offline]`).
- ✅ **Fritz!Fon-optimiert**: 
  - ISO-8859-1 Encoding für korrekte Darstellung.
  - CDATA-Blocks für Sonderzeichen (Pfeile `>` und `<`).
  - Automatische Umlaut-Konvertierung (ä -> ae etc.).
  - Anti-Cache-Header für minütliche Aktualisierung.
- ✅ **Richtungspfeile**: 
  - `>` für Züge Richtung Hannover.
  - `<` für Züge weg von Hannover (Haste/Nienburg).
- 🎉 **Humorvolle Feiertags-Meldungen**: Spezielle Verspätungsgründe für Ostern, Weihnachten, Vatertag, Star Wars Day, etc.
- 📱 **Health Check**: Leichtgewichtige `/health`-Route für stabiles Deployment auf Render.com.

## Fritz!Fon Setup
1. **FRITZ!Box** → Telefonie → DECT-Telefone → [dein Fon] → RSS-Feed.
2. **URL einfügen**: `https://abfahrten-wennigsen-bhf.onrender.com/feed`
3. **Intervall**: 1 Minute.
4. Fertig! 🥳

## Anzeige-Format
**Titel:** `HH:MM (+Verspätung) | Linie (Gleis) [Richtungspfeil] Ziel`  
**Details:**
- **Grund:** Verspätungsursache (humorvoll an Feiertagen).
- **Halte:** Liste der nächsten Stationen (mit `~~` bei Offline-Daten).
- **Info:** Allgemeine Hinweise (Fahrradmitnahme, etc.).

## Deployment
Das Projekt ist für **Render.com** optimiert:
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn rss_server:app`
- **Python Version:** 3.12.4 (via `runtime.txt`)

## Datenquellen
- [ÜSTRA Echtzeit](https://abfahrten.uestra.de)
- [DB Transport REST API](https://v6.db.transport.rest)

---
**Made with ❤️ für Pendler in Wennigsen** | Stand: März 2026
