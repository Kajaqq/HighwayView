# HighwayView Architecture

HighwayView has two related pipelines:

- A camera pipeline that downloads provider camera metadata, normalizes it into
  one JSON shape, verifies camera media, selects a curated loop, and writes OBS
  slideshow HTML.
- A traffic-alert pipeline that downloads DATEX II or CCISS alert feeds,
  normalizes them into alert models, filters stale or low-value records, and
  writes overlay JSON and JavaScript for OBS Browser Source.

The project keeps provider-specific behavior at the edges. Shared stages work on
normalized data so camera checking, loop creation, HTML generation, and alert
overlay export do not need to know the details of every upstream source.

## Camera Dataflow

```text
main.py
  -> Parsers/<country>_parser.get_parsed_data()
       -> Downloaders/<country>_downloader.get_data()
       -> Parsers/<country>_parser.parse()
       -> Parsers.base_parser.BaseParser.format_camera()
       -> Parsers.base_parser.BaseParser.format_highway_output()
  -> tools.camera_check.main()
       -> tools.utils.create_url()
       -> Downloaders.base_downloader.GenericDownloader.get_settings()
       -> tools.diff_hash.folder_hash()
       -> data/cameras_<country>_online.json
  -> tools.create_camera_loop.main()
       -> configured highway sequence in config.py
       -> selected camera ids
  -> tools.create_html.main()
       -> data/html/<country>_slideshow.html
  -> tools.serve_obs
       -> hourly refresh through main.get_camera_data()
       -> NL and BE camera media proxies with provider Referer headers
       -> in-memory OBS slideshow HTML from data/cameras_<country>_online.json
```

What flows through the camera pipeline:

- Provider metadata enters as raw JSON, JavaScript, HTML, or obfuscated text.
- Parsers normalize it into a list of highway objects:
  `{"highway": {"name": str, "country": str, "cameras": list[dict]}}`.
- Each camera is shaped by `BaseParser.format_camera()` with `camera_id`,
  `camera_km_point`, `camera_view`, `camera_type`, and `coords`.
- `tools.camera_check` removes offline cameras and optionally saves sampled media
  under `data/images/`.
- `tools.create_camera_loop` uses country-specific highway priorities from
  `config.py` to select a compact loop for OBS.
- `tools.create_html` turns normalized camera records or selected IDs into a
  self-contained slideshow page.
- Belgium has a provider-specific media split: normalized records keep the
  internal stream name as `camera_id`, `camera_check` validates the snapshot
  JPEG, and slideshow/runtime display uses the iframe player URL.

Why the flow is split this way:

- Downloaders own network access and source quirks that happen before parsing.
- Parsers own provider schema interpretation and normalization.
- Tools own post-normalization processing that should work across countries.
- `config.py` owns URLs, output paths, rate limits, route lists, and tuning data
  so behavior changes do not get scattered across pipeline code.

## Traffic Alert Dataflow

```text
get_datex.py
  -> per-country COUNTRY_CONFIGS
  -> DatexParser.datex_parser.DatexParser or DatexParser.cciss_parser.CcissParser
       -> Downloaders.base_downloader.GenericDownloader.download()/download_bytes()
       -> optional NDW VILD SQLite enrichment for NL Alert-C LOC_NR
       -> TruckDashboardAlert models
  -> DatexParser.overlay_export.build_overlay_payload()
       -> optional road whitelist
       -> medium-or-higher / road-closed relevance gate
       -> DatexParser.datex_filter.HeuristicFilter
  -> DatexParser.overlay_export.write_overlay_payload()
       -> data/overlay_<country>/overlay_data.json
       -> data/overlay_<country>/overlay_data.js
  -> data/overlay_<country>/index.html
  -> tools.serve_obs
       -> five-minute refresh through get_datex.py helpers
       -> local serving of static overlay assets
```

What flows through the alert pipeline:

- Spain, France, and the Netherlands use
  `DatexParser.datex_parser.DatexParser` for DATEX II XML.
- The Netherlands feed is published as gzipped DATEX II v3 XML. The parser
  decompresses `.gz` downloads, reads Alert-C/VILD `specificLocation` values,
  and enriches them from `DatexParser/data/vild_6.13.A.sqlite`.
- Italy uses `DatexParser.cciss_parser.CcissParser` for CCISS traffic event JSON.
- Both parser families produce `TruckDashboardAlert` instances from
  `DatexParser.datex_models`.
- Dutch VILD data maps into the existing shared alert fields: road number into
  `road_name`, VILD location name into `road_destination`, `LOC_NR` into
  `reference_marker`, `Provincie` into `province`, `Gemeente` into
  `municipality`, and `Plaats` into `community`. VILD hectometer ranges and
  DATEX offsets are used internally to estimate `km_point`.
- `DatexParser.overlay_export` serializes alerts into browser-friendly payloads.
- `DatexParser.datex_filter.HeuristicFilter` classifies records as active,
  suspicious, or stale based on alert age, cause, severity, and management type.

Why the alert flow is separate from cameras:

- Alert feeds have different source formats, freshness rules, and display
  contracts than camera metadata.
- The overlay layer needs alert-specific filtering and confidence classification.
- OBS overlay assets live beside their runtime payloads under `data/overlay_*`.

## Module Responsibilities

| Location | Responsibility | Change here when |
| --- | --- | --- |
| `main.py` | Orchestrates the full camera workflow for all configured countries. | You need to change stage ordering or which country pipelines run. |
| `get_datex.py` | Orchestrates alert overlay export once or in a refresh loop. | You need to change alert CLI behavior, country routing, road defaults, or per-country filter settings. |
| `config.py` | Central constants for source URLs, output paths, rate limits, route sequences, overlay directories, and provider tuning. | A provider URL, route whitelist, slideshow interval, rate limit, or output location changes. |
| `Downloaders/` | Network access and source-specific download/decode steps. | Fetching raw provider data changes, headers/timeouts/rate limits need reuse, or a source adds obfuscation. |
| `Parsers/` | Camera provider schema parsing and normalization into the shared highway/camera structure. | Raw camera metadata shape changes or a new camera source must be normalized. |
| `DatexParser/datex_models.py` | Pydantic models for normalized traffic alert records. | The shared alert contract needs a new field or stricter validation. |
| `DatexParser/datex_parser.py` | Spanish, French, and Dutch DATEX II XML parsing, gzipped feed handling, and Dutch VILD enrichment. | DATEX XML schema handling, record extraction, VILD field mapping, or feed download behavior changes. |
| `DatexParser/cciss_parser.py` | Italian CCISS event parsing into the shared alert model. | CCISS JSON fields, text extraction, or road/cause mapping changes. |
| `DatexParser/datex_filter.py` | Alert freshness and confidence classification rules. | Stale-alert TTLs, severity gates, or suspicious/zombie behavior changes. |
| `DatexParser/overlay_export.py` | Alert payload construction and writing `overlay_data.json` plus `overlay_data.js`. | Browser payload shape, sorting, max item handling, or overlay output format changes. |
| `DatexParser/vild_dbf_to_sqlite.py` | Converts NDW VILD DBF attributes into a compact SQLite `LOC_NR` lookup table. | The VILD source version changes or the parser needs additional non-geometry VILD attributes. |
| `DatexParser/data/vild_*.sqlite` | Derived NDW VILD lookup data used at parse time for Dutch Alert-C location references. | Updating the bundled VILD release used by the Netherlands DATEX parser. |
| `tools/camera_check.py` | Camera media verification, offline filtering, and duplicate/error-image detection. | Online/offline detection, sample downloads, or checked JSON output changes. |
| `tools/create_camera_loop.py` | Country-specific route ordering and sampled camera loop selection. | OBS loop composition or highway ordering changes. |
| `tools/create_html.py` | Slideshow HTML generation from normalized camera records. | Browser slideshow behavior, markup, styling, or media loading changes. |
| `tools/serve_obs.py` | Local aiohttp runtime server for OBS Browser Source URLs over generated artifacts, including NL and BE media proxy routes that attach provider `Referer` headers. | Serving routes, local response behavior, or refresh scheduling changes. |
| `tools/utils.py` | Shared JSON, URL, coordinate, country, and distance helpers. | A cross-cutting primitive is reused by multiple pipeline stages. |
| `data/` | Runtime output and static overlay assets. | Usually do not change generated JSON or sampled media as source behavior. Static overlay HTML/CSS/JS can be changed when UI behavior changes. |
| `docs/` | Human architecture notes and generated API documentation. | You need project-level guidance or regenerated reference docs. |

## Data Contracts

Camera records are grouped by highway:

```json
[
  {
    "highway": {
      "name": "A-7",
      "country": "ES",
      "cameras": [
        {
          "camera_id": "123",
          "camera_km_point": 42.0,
          "camera_view": "*",
          "camera_type": "img",
          "coords": {"X": -3.5, "Y": 40.4}
        }
      ]
    }
  }
]
```

Alert records are modeled as `TruckDashboardAlert` and exported as plain JSON
objects with severity, cause, management, road, time, location, confidence, and
comments fields. Location objects expose only the shared display contract:
coordinates, `km_point`, `reference_marker`, `offset_m`, `community`,
`province`, and `municipality`. Raw Alert-C/VILD diagnostic fields and VILD
`km_start_*` / `km_end_*` ranges stay internal to parsing and are not exported.
The browser overlay consumes the exported payload rather than parsing DATEX,
CCISS, or VILD directly.

## What / Why / Where

- Add a new camera provider: put network retrieval in `Downloaders/`, parsing in
  `Parsers/`, constants in `config.py`, and wire orchestration through `main.py`
  only after the normalized output matches the existing highway/camera contract.
- Change camera media URL generation: update `tools.utils.create_url()` for
  countries whose final media URL is derived from normalized camera IDs; update a
  parser only when the upstream metadata itself changed.
- Change provider-specific camera checking: update `tools/camera_check.py` when
  the verification URL differs from the display URL, as with Belgium snapshots
  versus iframe playback.
- Change camera availability rules: update `tools/camera_check.py`, because it is
  the stage that validates media responses and removes offline records.
- Change OBS slideshow behavior: update `tools/create_html.py` for rendering and
  loading behavior, or `tools/create_camera_loop.py` for which cameras enter the
  loop.
- Change local OBS serving behavior: update `tools/serve_obs.py`, keeping it as
  a presentation/runtime layer over existing generated artifacts. BE iframe,
  player asset, and HLS URLs are proxied locally so the provider `Referer`
  header can be sent consistently.
- Change traffic alert source parsing: update `DatexParser/datex_parser.py` for
  Spain, France, or Netherlands DATEX XML, or `DatexParser/cciss_parser.py` for
  Italy CCISS.
- Change Dutch VILD lookup fields: update `DatexParser/vild_dbf_to_sqlite.py`
  for DBF-to-SQLite conversion and `DatexParser/datex_parser.py` for how those
  fields map onto the shared alert model.
- Change alert filtering policy: update `DatexParser/datex_filter.py` and the
  per-country `FilterConfig` values in `get_datex.py`.
- Change overlay payload shape or output files: update
  `DatexParser/overlay_export.py`; update `data/overlay_*` static assets only
  when the browser UI needs to consume or display the new shape.
- Change URLs, route lists, rate limits, or generated output paths: update
  `config.py` first.

## Generated Outputs

The pipeline writes runtime artifacts under `data/`, including checked camera
JSON, sampled media, slideshow HTML, and alert overlay payloads. Treat these as
outputs unless a file is a static overlay asset such as `data/overlay_*/index.html`,
`styles.css`, or `overlay.js`.

`DatexParser/data/vild_*.sqlite` is different from runtime overlay output: it is
derived reference data for parsing Dutch Alert-C/VILD location IDs. Rebuild it
from the NDW VILD DBF with `DatexParser/vild_dbf_to_sqlite.py` when changing the
VILD release.
