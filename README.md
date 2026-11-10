# OpenShipTracker

**OpenShipTracker** is a lightweight, self-hosted web application for tracking vessels in real-time using live AIS data and visualizing historical voyages on an interactive map.

---

## Highlights

- **Real-Time AIS Stream**: Ingest live vessel positions directly from [AISStream.io](https://aisstream.io) via WebSocket.
- **Native iOS App**: Full-featured SwiftUI & MapKit app with Onboarding Setup-Wizard, standalone live WebSocket client, voyage tracking, and server sync.
- **Interactive Dark Map**: Leaflet-based map with Dark Matter tiles, OpenSeaMap nautical marks, heading vectors, and live position markers.
- **Voyage & Track Filtering**: Inspect historical voyages with flexible time range presets (24h, 7d, 30d, all, custom date/time) and automatic nautical mile and speed calculations.
- **Custom Fleet Management**: Add, categorize, and track custom ships by MMSI.
- **Self-Contained Storage**: Uses SQLite with zero external database dependencies.
- **Docker Ready**: One-click deployment with Docker Compose.

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
