# HighwayView

## Overview

HighwayView is a toolkit designed to scrap and aggregate the highway cameras from major European countries.

It handles scraping, aggregation, deduplication, verification and visualization.

## Supported Functionalities

- **Scraping & Parsing**: Extract camera data and media URLs from multiple European highway agencies into a unified structured JSON format.
- **Verification & Deduplication** (`camera_check.py`): Automatically check camera availability, filter out offline feeds, and remove visually duplicate images or error screens.
- **Slideshow Generation** (`create_html.py`): Create optimized HTML slideshows from the camera data. Features lazy loading, memory management, and error handling, making it ideal for streaming software like OBS.
- **Curated Camera Loops** (`create_camera_loop.py`): Automatically generate ~10-minute curated cycles of the most important national highways for each supported country.
- **Data Inspection** (`list_cameras.py`): Command-line utility to quickly summarize highway and camera counts from parsed datasets.

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

## Project Structure

The project is split into three modules and an orchestration script.

- `main.py`: Orchestrates the scraping, parsing, checking, and visualization of camera data.
- `Downloaders/`: Contains the scraping module for each country.
- `Parsers/`: Contains the parsing module for each country.
- `tools/`: Contains the tools for checking, and visualizing camera data.
- `data/`: Contains the raw and processed camera data.

## Documentation

The project includes API reference documentation generated from Python docstrings.
To explore the documentation, open the [`docs/index.html`](docs/index.html) file in your web browser.

## Supported Sources

- **France**
  - Bison Futé
  - ASFA (Association des Sociétés Françaises d'Autoroutes)
- **Italy**
  - Autostrade de Italia
  - Autostrada del Brennero
  - Autostrada Brescia Verona Vicenza Padova (ABP)
  - Concessioni Autostradali Venete (CAV)
  - SATAP (Società Autostrada Torino-Alessandria-Piacenza S.p.A.)
- **Spain**
  - DGT (Dirección General de Tráfico)
- **UK**
  - Highways England
