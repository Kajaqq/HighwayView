# HighwayView

## Overview

HighwayView is a toolkit designed to scrape and aggregate highway cameras from
major European countries.

It handles scraping, aggregation, deduplication, verification, visualization,
and DATEX II / CCISS traffic alert overlays for OBS Browser Source.

## Supported Functionalities

- **Scraping & Parsing**: Extract camera data and media URLs from multiple European highway agencies into a unified structured JSON format.
- **Verification & Deduplication** (`camera_check.py`): Automatically check camera availability, filter out offline feeds, and remove visually duplicate images or error screens.
- **Slideshow Generation** (`create_html.py`): Create optimized HTML slideshows from the camera data. Features lazy loading, memory management, and error handling, making it ideal for streaming software like OBS.
- **Curated Camera Loops** (`create_camera_loop.py`): Automatically generate ~10-minute curated cycles of the most important national highways for each supported country.
- **Data Inspection** (`list_cameras.py`): Command-line utility to quickly summarize highway and camera counts from parsed datasets.
- **DATEX II / CCISS Overlays** (`DatexParser module`): Export traffic alerts for Spain, France, Italy, and the Netherlands to per-country OBS overlay data files. Netherlands DATEX II alerts are enriched from NDW VILD Alert-C location data.

## Usage Examples

Ensure you have the `uv` package manager installed.

**Run the Full Pipeline**
To run the main orchestration script that downloads, parses, checks cameras, and creates HTML slideshows:

```bash
uv run main.py
```

**Generate HTML Slideshows for Specific Highways**
You can use the HTML generator to filter for specific routes and set a custom interval (e.g., Spain's AP-7 and A-7 with 10s intervals):

```bash
uv run tools/create_html.py data/cameras_es_online.json --highways AP-7,A-7 --interval 10
```

**Generate HTML Slideshows for Specific Cameras**
Only include specific camera IDs from the UK dataset:

```bash
uv run tools/create_html.py data/cameras_uk_online.json -c cam_m25_1 cam_m25_2
```

**Verify a Dataset**
Identify offline or broken cameras and output a clean JSON dataset:

```bash
uv run tools/camera_check.py data/france_original.json
```

**List Camera Counts**
Print a formatted list of all highways and their valid cameras from a generated dataset:

```bash
uv run tools/list_cameras.py data/cameras_it_online.json
```

**Serve OBS Browser Sources Locally**
Serve current generated camera and overlay artifacts through a local aiohttp server:

```bash
uv run tools/serve_obs.py
```

Use these URLs in OBS Browser Source:

```text
http://127.0.0.1:8765/cameras/NL
http://127.0.0.1:8765/cameras/BE
http://127.0.0.1:8765/overlay/ES/
```

NL and BE camera media are proxied through the local server so HighwayView can
attach provider `Referer` headers when fetching the actual camera feeds. BE
slideshow iframes use `/proxy/cameras/BE/?name=<camera_id>`; the local server
then proxies the BE player assets and HLS stream URLs.

### DatexParser module examples 
**Generate Traffic Alert Overlays**
Generate overlay data for all configured DATEX II / CCISS countries once:

```bash
uv run get_datex.py --once
```

Generate a single country overlay:

```bash
uv run get_datex.py --country ES --once
```

Generate the Netherlands overlay from the NDW gzipped DATEX II feed:

```bash
uv run get_datex.py --country NL --once
```

Use a custom road whitelist:

```bash
uv run get_datex.py --country FR --roads A7,A16 --once
```

Disable heuristic filtering:

```bash
uv run get_datex.py --no-filter --once
```

Run continuously (refresh every 5 minutes by default):

```bash
uv run get_datex.py
```

Use the per-country overlay UI files:

```text
data/overlay_spain/index.html
data/overlay_france/index.html
data/overlay_italy/index.html
data/overlay_netherlands/index.html
```

**Netherlands VILD lookup**

The Netherlands DATEX parser uses `DatexParser/data/vild_6.13.A.sqlite` to
translate Alert-C/VILD `LOC_NR` values into road names, nearby areas, and
administrative fields. The VILD value is exported as `reference_marker`; VILD
hectometer ranges and DATEX offsets are used internally to estimate `km_point`.
Raw `alertc_*` fields and VILD `km_start_*` / `km_end_*` ranges are not written
to `overlay_data.json`.

Rebuild the SQLite lookup from the NDW VILD DBF when updating VILD versions:

```bash
uv run DatexParser/vild_dbf_to_sqlite.py path/to/VILD6.13.A.dbf DatexParser/data/vild_6.13.A.sqlite
```

## Project Structure

The project is split into three modules and an orchestration script.

- `main.py`: Orchestrates the scraping, parsing, checking, and visualization of camera data.
- `get_datex.py`: Utilizes DatexParser to export DATEX II / CCISS traffic alerts to JSON files.
- `config.py`: Stores country-specific constants and shared settings.
- `Downloaders/`: Contains the scraping module for each country.
- `Parsers/`: Contains the parsing module for each country.
- `DatexParser/`: Contains DATEX II XML parsing, Italian CCISS parsing, Dutch VILD lookup generation, heuristic filtering, and overlay export code.
- `tools/`: Contains the tools for checking, visualizing, and locally serving OBS runtime artifacts.
- `data/`: Contains the raw and processed camera data plus per-country overlay assets.

## Documentation

- **Architecture**: [`docs/Architecture.md`](docs/Architecture.md) — subsystem design, data flow, and component overview.
- **API Reference**: Generated from docstrings. Open [`docs/index.html`](docs/index.html) in a browser.

## Supported Sources 

- **France**
  - Bison Futé
  - ASFA (Association des Sociétés Françaises d'Autoroutes)
  - DATEX II Feed
- **Italy**
  - Autostrade de Italia
  - Autostrada del Brennero
  - Autostrada Brescia Verona Vicenza Padova (ABP)
  - Concessioni Autostradali Venete (CAV)
  - SATAP (Società Autostrada Torino-Alessandria-Piacenza S.p.A.)
  - CCISS Feed
- **Spain**
  - DGT (Dirección General de Tráfico)
  - DATEX II Feed
- **Netherlands**
  - Rijkswaterstaat cameras
  - NDW DATEX II actueel beeld feed
  - NDW VILD location reference data
- **Belgium**
  - Vlaams Verkeerscentrum cameras
- **UK**
  - Highways England
