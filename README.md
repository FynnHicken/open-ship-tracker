# OpenShipTracker – AIS Live Radar, Fleet Filter & MMSI Database

**OpenShipTracker** is a self-hosted web application for tracking all vessels in a configurable sea area (default: German Bight, Elbe/Weser/NOK & Western Baltic) in real-time via AISStream, storing a 30-day rolling track archive, and maintaining a permanent MMSI ship database.

---

## Highlights

- **Regional AIS Live Stream & Interactive Search Area (`Suchbereich`)**: Ingest live vessel positions (Class A & Class B) directly from [AISStream.io](https://aisstream.io) via WebSocket. Drag and resize the search area directly on the map using corner handles.
- **Fleet Filter & Fleet Manager (`⚓ Flotten`)**:
  - Pre-configured **Deutsche Marine** fleet (71 naval units with hull numbers & names).
  - Personal **⭐ Meine Flotte** bookmarking with one click from any map popup or database row.
  - Create custom fleets (e.g., club boats, tugs, SAR) and filter the live map or database by fleet.
- **30-Day Rolling Track History**: Intelligent downsampling stores 30 days of vessel routes while keeping SQLite fast and compact.
- **Pre-Populated MMSI Ship Database**:
  - `data/bight.db`: SQLite database containing **3,330+ cataloged vessels** and historical tracks.
  - `data/mmsi_database.json`: Ready-to-use JSON export of all cataloged MMSIs with ship name, callsign, IMO, dimensions, flag, and ship category.
- **Native iOS App**: Full-featured SwiftUI & MapKit app located in `ios/`.

---

## Quick Start with Docker

1. **Clone the repository**:
   ```bash
   git clone https://github.com/FynnHicken/open-ship-tracker.git
   cd open-ship-tracker
   ```

2. **Configure environment variables (optional)**:
   ```bash
   cp .env.example .env
   ```
   Add your free [AISStream.io API key](https://aisstream.io) inside `.env`:
   ```env
   AISSTREAM_API_KEY=your_key_here
   ```

3. **Start with Docker Compose**:
   ```bash
   docker compose up -d --build
   ```

4. **Open in browser**:
   Navigate to `http://localhost:8050`.

---

## Manual Installation (Python)

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run server
export DATA_DIR="./data"
uvicorn app.main:app --host 0.0.0.0 --port 8050
```

---

## iOS App (Xcode)

A fully native iOS application (SwiftUI / MapKit) is located inside `ios/`:

```bash
open ios/OpenShipTracker.xcodeproj
```

Features an interactive **Setup Wizard** upon first launch to configure your AISStream API key, choose between direct standalone tracking or connecting to your self-hosted server, and select your starter fleet.

---

## Architecture

- **Backend**: FastAPI (Python 3.11), Uvicorn, SQLite3, WebSockets
- **Frontend**: Leaflet.js, Tailwind CSS, OpenSeaMap nautical layer
- **Data Ingestion**: Async WebSocket listener for AISStream + optional REST lookups

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
