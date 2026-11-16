Dashboard for MMDVM using MQTT protocol
==
This project uses Jonathan G4KLX's latest implementations of the MMDVM suite.

Currently, the entire logging system is managed via the MQTT protocol, so a "broker" like Mosquitto must be installed to route the log streams.

Topic must be in the format mmdvm/[nodename] for MMDVMHost.ini

For [MODE]Gateway.ini, topic format must be: [mode]-gateway/[nodename]

You can find an example here: http://mqtt.freedmr.it:7000

Documentazione Tecnica / Technical Documentation
==

### [ITALIANO]

> DESCRIZIONE GENERALE

Questo progetto è una dashboard in tempo reale per monitorare il traffico radio
digitale (DMR, YSF, NXDN, P25, D-STAR) che transita attraverso gateway MMDVM 
tramite protocollo MQTT.

> ARCHITETTURA

L'applicazione segue un'architettura client-server moderna:
1. Backend (Python/Flask): Gestisce la ricezione dei dati e le API.
2. Database (SQLite): Memorizza lo storico delle chiamate e le anagrafiche utenti.
3. Frontend (HTML5/JS/CSS3): Visualizza i dati in tempo reale con una 
   grafica moderna (Glassmorphism).

> LOGICA DI FUNZIONAMENTO (BACKEND)
- mqtt_parser.py: Si connette al broker MQTT e ascolta i topic configurati. 
  Quando riceve un messaggio JSON, lo analizza, identifica il modo digitale, 
  arricchisce i dati cercando il nominativo (callsign) nel database locale e 
  salva l'evento. Gestisce anche lo stato dei gateway (link/unlink).
- app.py: Fornisce le API REST per il frontend. Le API principali sono:
  * /data: Restituisce le ultime chiamate e lo stato dei nodi.
  * /stats: Fornisce dati aggregati per i grafici.

> LOGICA DI FUNZIONAMENTO (FRONTEND)
- La pagina carica i dati ogni secondo tramite chiamate asincrone (AJAX/Fetch).
- La visualizzazione è divisa in tre sezioni principali:
  1. Monitor Live: Tabella e schede dei nodi attivi.
  2. Gateway Status: Elenco dello stato di connessione dei vari gateway.
  3. Analytics & Map: Grafici statistici e mappa mondiale dei transiti.

> LOGICA DEI PULSANTI E DEI FILTRI (FRONTEND)
- Navigazione (Main Nav Bar): Gestisce la visibilità delle diverse "viste" 
  dell'app. Quando si cambia tab, l'app disabilita o abilita automaticamente 
  i filtri non pertinenti per migliorare l'esperienza utente.
- Filtri Sorgente (ALL/MMDVM/GATEWAY): Filtrano i record in base alla colonna 
  `SOURCE_TYPE`. Questo permette di isolare il traffico nativo MMDVM da quello 
  proveniente da gateway esterni.
- Filtri di Modo (DMR, YSF, ecc.): Permettono di isolare rapidamente un singolo 
  protocollo digitale.
- Filtri a Tendina (Dropdown): Forniscono una ricerca granulare per nodo, 
  Talkgroup o nome utente. Questi menu si popolano dinamicamente in base ai 
  dati ricevuti.

> SICUREZZA
- Tutte le query al database utilizzano parametri (parameterized queries) per 
  prevenire SQL Injection.
- I percorsi dei file sono gestiti in modo robusto tramite la libreria pathlib.

--------------------------------------------------------------------------------

### [ENGLISH]

> GENERAL DESCRIPTION

This project is a real-time dashboard designed to monitor digital radio traffic 
(DMR, YSF, NXDN, P25, D-STAR) traversing MMDVM gateways via the MQTT protocol.

> ARCHITECTURE

The application follows a modern client-server architecture:
1. Backend (Python/Flask): Handles data reception and provides APIs.
2. Database (SQLite): Stores call history and user identification data.
3. Frontend (HTML5/JS/CSS3): Displays real-time data with a premium 
   modern design (Glassmorphism).

> OPERATIONAL LOGIC (BACKEND)
- mqtt_parser.py: Connects to the MQTT broker and listens to configured topics. 
  Upon receiving a JSON message, it parses it, identifies the digital mode, 
  enriches the data by looking up the callsign in the local database, and 
  saves the event. It also manages gateway connection states (link/unlink).
- app.py: Serves REST APIs to the frontend. Key APIs include:
  * /data: Returns the latest calls and node statuses.
  * /stats: Provides aggregated data for analytics and charts.

> OPERATIONAL LOGIC (FRONTEND)
- The page fetches data every second using asynchronous calls (Fetch API).
- The UI is organized into three main sections:
  1. Live Monitor: Table and cards showing active transmissions.
  2. Gateway Status: List of current connection statuses for various gateways.
  3. Analytics & Map: Statistical charts and a world map of transmissions.

> BUTTONS AND FILTERS LOGIC (FRONTEND)
- Navigation (Main Nav Bar): Switches between dashboard views. Filters are 
  automatically enabled or disabled based on the current tab to ensure a 
  consistent user experience.
- Source Filters (ALL/MMDVM/GATEWAY): Separates traffic based on the 
  `SOURCE_TYPE` column (e.g., native MMDVM vs external gateways).
- Mode Filters (DMR, YSF, etc.): Quickly isolates traffic from a specific 
  digital protocol.
- Dropdown Filters: Provide granular search by node, Talkgroup, or username. 
  These menus are dynamically populated based on the received data.

> SECURITY
- All database queries use parameterized statements to prevent SQL Injection.
- File paths are managed robustly using Python's pathlib library.

### © 2026  FreeDMR Italia - Developed by FreeDMR-IT Dev Team

Configurazione opzionale / Optional settings (.env)
==
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`: alert Telegram quando un nodo non invia telemetria (`NODE_TIMEOUT_HOURS`, default 3) e quando torna online. Vedi `Telegram.txt`.
- `KEEP_HISTORY=1`: conserva la cronologia chiamate tra i riavvii; `RETENTION_DAYS` (default 30) elimina i record più vecchi. Senza, la cronologia viene svuotata all'avvio.
- `WAITRESS_THREADS` (default 64): thread del server; ogni client aperto della dashboard ne occupa uno (SSE).
- `ADMIN_TOKEN`: abilita `POST /clear` (header `X-Admin-Token`).

Test
==
```
pip install -r requirements.txt
python -m unittest discover -s tests
```

Novità / Features
==
- Esporta Cronologia con filtro date: `/api/export_calls?from=YYYY-MM-DD&to=YYYY-MM-DD` (estremi inclusi, entrambi opzionali).
- Ricerca per nominativo, nome o ID nella barra dei filtri.
- Statistiche storiche (tab Analytics): chiamate per ora, giorno e nodo, utenti e TG più attivi con tempo in aria. API: `/api/stats?days=7` (1-365). Per periodi lunghi serve `KEEP_HISTORY=1`.
