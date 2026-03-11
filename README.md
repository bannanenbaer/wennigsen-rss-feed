## README.md für dein GitHub‑Repo

```markdown
# 🚋 Wennigsen (Deister) Bahnhof RSS‑Feed

Optimiertes RSS‑Feed für **Fritz!Fon** mit Echtzeit‑Abfahrten vom Bahnhof Wennigsen.

## Features
- ✅ **Primär**: ÜSTRA‑API (Echtzeit, Plattform, Hinweise)  
- ✅ **Anreicherung**: DB/VBN (Zwischenhalte, Ausfälle, Remarks)  
- ✅ **Fallbacks**: DB → VBN  
- ✅ **Fritz!Fon‑optimiert**: ISO‑8859‑1, Umlaute → ae/oe/ue/ss, kompakte Titel  
- 🎉 **Easter Eggs**: Feiertags‑spezifische Meldungen (Ostern, Weihnachten, Vatertag, etc.)  
- 📱 **Health Check**: `/health` für Monitoring  

## Live‑Demo
```
https://wennigsen-rss-[dein-random].onrender.com/feed.rss
```

## Fritz!Fon Setup
1. **FRITZ!Box → Telefonie → DECT‑Telefone → [dein Fon] → RSS‑Feed**  
2. **URL einfügen**: `https://deine-url.onrender.com/feed.rss`  
3. **Intervall**: 1 Minute  
4. Fertig! 🥳

## Screenshots
```
Titel: 16:03 (+2) | S1 (Gl.1) > Hannover Hbf.
Beschreibung: +2 Min (plan: 16:03, neu: 16:05)
Grund: Lokfuehrer sucht noch Ostereier (Personalmangel)
Halte: 16:10 Seelze Bhf.
```

## Deployment (selbst hosten)
### Render.com (empfohlen, kostenlos)
```
1. Fork dieses Repo
2. render.com → New → Web Service → GitHub Repo
3. Build: pip install -r requirements.txt
4. Start: Procfile übernimmt automatisch
```

### Lokales Testen
```bash
pip install -r requirements.txt
python app.py
# http://localhost:5000/feed.rss
```

## API‑Endpoints
| Endpoint | Beschreibung |
|----------|--------------|
| `/feed.rss` | **Haupt‑RSS‑Feed** (Fritz!Fon) |
| `/` | Landing Page |
| `/health` | Status + Stats (Deploy‑Healthcheck) |

## Datenquellen
- [ÜSTRA Echtzeit](https://abfahrten.uestra.de)  
- [DB Transport REST API](https://v6.db.transport.rest)  
- [VBN Transport REST](https://v6.vbn.transport.rest)

## License
MIT – tu damit, was du willst! 🎉

---
**Made with ❤️ für Pendler in Wennigsen** | Stand: März 2026
```
