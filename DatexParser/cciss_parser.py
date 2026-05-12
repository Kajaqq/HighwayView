"""CCISS traffic event parser for Italian road alerts.

Fetches JSON from the CCISS (Centro Coordinamento Informazioni sulla
Sicurezza Stradale) Liferay portlet API and maps each event to a
:class:`TruckDashboardAlert`, making it compatible with the existing
``HeuristicFilter`` → ``overlay_export`` pipeline.

Example::

    parser = CcissParser(downloader=GenericDownloader())
    alerts = await parser.get_parsed_data()
    a1_alerts = parser.filter_by_road("A1")
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from config import CONSTANTS
from Downloaders.base_downloader import GenericDownloader
from tools.utils import haversine_km

from .datex_models import LocationPoint, TruckDashboardAlert

_ROME_TZ = CONSTANTS.ITALY.ROME_TZ

_ROAD_CODE_RE = re.compile(
    r"^((?:GRA|RA|SS|SR|SP|SGC|NSA|A|E)\d+)",
    re.IGNORECASE,
)

_TRATTO_TWO_RE = re.compile(
    r"tra\s+(?:Incrocio|Svincolo|Galleria|Barriera|Allacciamento|SP\d+|SS\d+|\d[\d,]+\s*km\s+dopo\s+\w+)\s+(.+?)\s+e\s+(?:Incrocio|Svincolo|Galleria|Barriera|Allacciamento|SP\d+|SS\d+|Strada\s+Statale)\s+(.+)",
    re.IGNORECASE,
)
_TRATTO_SINGLE_RE = re.compile(
    r"a\s+(?:Incrocio|Svincolo|Galleria|Barriera)\s+(.+)",
    re.IGNORECASE,
)

_CAUSE_RE = re.compile(r"causa\s+(\w+)", re.IGNORECASE)

_START_DATE_RE = re.compile(
    r"dalle\s+(\d{1,2}:\d{2})\s+del\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
    re.IGNORECASE,
)
_END_DATE_RE = re.compile(
    r"(?:^|\s)alle\s+(\d{1,2}:\d{2})\s+del\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
    re.IGNORECASE,
)

_ITALIAN_MONTHS: dict[str, int] = {
    "gennaio": 1,
    "febbraio": 2,
    "marzo": 3,
    "aprile": 4,
    "maggio": 5,
    "giugno": 6,
    "luglio": 7,
    "agosto": 8,
    "settembre": 9,
    "ottobre": 10,
    "novembre": 11,
    "dicembre": 12,
}

_PRIORITY_TO_SEVERITY: dict[int, str] = {
    4: "highest",
    3: "high",
    2: "medium",
    1: "low",
}

_EVENT_TO_MANAGEMENT: dict[str, str] = {
    "tratto chiuso": "roadClosed",
    "chiuso": "roadClosed",
    "chiusura rampa": "roadClosed",
    "senso unico alternato": "singleAlternateLineTraffic",
    "restringimento carreggiata": "narrowLanes",
    "carreggiata ridotta": "narrowLanes",
    "deviazione": "roadDeviation",
    "code": "congestion",
    "code a tratti": "congestion",
    "traffico rallentato": "slowTraffic",
    "traffico bloccato": "stationaryTraffic",
}

_CAUSE_KEYWORD_MAP: dict[str, str] = {
    "lavori": "roadMaintenance",
    "incidente": "accident",
    "frana": "infrastructureDamageObstruction",
    "smottamento": "infrastructureDamageObstruction",
    "maltempo": "poorWeatherConditions",
    "neve": "poorWeatherConditions",
    "ghiaccio": "poorWeatherConditions",
    "pioggia": "poorWeatherConditions",
    "nebbia": "poorWeatherConditions",
    "animali": "animalPresence",
    "veicolo": "vehicleObstruction",
    "manifestazione": "abnormalTraffic",
}


class CcissParser:
    """Parser for CCISS Italian traffic event JSON.

    After calling :meth:`get_parsed_data`, parsed alerts are stored
    internally and can be queried via :meth:`filter_by_road` and
    :meth:`filter_by_location`.

    Args:
        downloader: HTTP downloader instance.
    """

    def __init__(self, downloader: GenericDownloader | None = None) -> None:
        self.downloader = downloader or GenericDownloader()
        self._alerts: list[TruckDashboardAlert] = []
        self._url = self._build_url()

    @property
    def country(self) -> str:
        return "IT"

    @property
    def alerts(self) -> list[TruckDashboardAlert]:
        return self._alerts

    @staticmethod
    def _build_url() -> str:
        base = CONSTANTS.ITALY.CCISS_URL
        params = CONSTANTS.ITALY.CCISS_PARAMS
        return f"{base}?{urlencode(params)}"

    async def get_parsed_data(
        self,
        output_file: str | Path | None = None,
        output_folder: str | Path | None = None,
    ) -> list[TruckDashboardAlert]:
        raw_text = await self.downloader.download(self._url)
        alerts = self.parse(raw_text)

        if output_file:
            self._save_alerts(alerts, Path(output_file))
        elif output_folder:
            self._save_alerts(alerts, Path(output_folder) / "cciss_alerts.json")

        return alerts

    def parse(self, raw_data: str) -> list[TruckDashboardAlert]:
        data = json.loads(raw_data)
        events: list[dict[str, Any]] = data.get("eventiTrafficoList", [])

        alerts: list[TruckDashboardAlert] = []
        for event in events:
            if event.get("stato") != "ACTIVE":
                continue
            alert = self._map_event(event)
            alerts.append(alert)

        self._alerts = alerts
        print(f"[IT] Parsed {len(alerts)} CCISS alerts.")
        return alerts

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_by_road(self, road: str) -> list[TruckDashboardAlert]:
        return [a for a in self._alerts if a.road_name and road == a.road_name]

    def filter_by_location(
        self, lat: float, lon: float, radius_km: float
    ) -> list[TruckDashboardAlert]:
        def _within(point: LocationPoint | None) -> bool:
            if point is None or point.latitude is None or point.longitude is None:
                return False
            return haversine_km(lat, lon, point.latitude, point.longitude) <= radius_km

        return [
            a
            for a in self._alerts
            if _within(a.location_from) or _within(a.location_to)
        ]

    # ------------------------------------------------------------------
    # Event → TruckDashboardAlert mapping
    # ------------------------------------------------------------------

    def _map_event(self, event: dict[str, Any]) -> TruckDashboardAlert:
        oid = str(event.get("oid", ""))
        dettaglio = event.get("dettaglio") or ""
        titolo = event.get("titolo") or ""
        evento = (event.get("evento") or "").strip().lower()

        creation_time = self._parse_publication_date(event.get("dataPubblicazione"))
        start_time, end_time = self._parse_detail_dates(dettaglio)
        if start_time is None:
            start_time = creation_time

        location_from = self._extract_location(event, "coordinateMappaInizioTO")
        location_to = self._extract_location(event, "coordinateMappaFineTO")

        tratto = (event.get("trattoEvento") or "").strip()
        name_from, name_to = self._parse_tratto(tratto)
        if name_from and location_from:
            location_from.municipality = name_from
        elif name_from and not location_from:
            location_from = LocationPoint(municipality=name_from)
        if name_to and location_to:
            location_to.municipality = name_to
        elif name_to and not location_to:
            location_to = LocationPoint(municipality=name_to)

        direction_raw = (event.get("direzioneEvento") or "").strip()
        road_destination = None
        if direction_raw:
            road_destination = (
                re.sub(
                    r"^in direzione\s+",
                    "",
                    direction_raw,
                    flags=re.IGNORECASE,
                ).strip()
                or None
            )

        return TruckDashboardAlert(
            situation_id=oid,
            record_id=oid,
            creation_time=creation_time,
            version_time=creation_time,
            severity=_PRIORITY_TO_SEVERITY.get(event.get("prioritaInt", 0)),
            start_time=start_time,
            end_time=end_time,
            management_type=self._map_management_type(evento),
            vehicle_type=None,
            cause_type=self._extract_cause(dettaglio, evento),
            detailed_cause_type=evento or None,
            road_name=self._extract_road_name(titolo),
            road_destination=road_destination,
            direction=None,
            carriageway=None,
            lane_usage=None,
            location_from=location_from,
            location_to=location_to,
            public_comments=[dettaglio] if dettaglio else [],
        )

    # ------------------------------------------------------------------
    # Field extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_road_name(titolo: str) -> str | None:
        m = _ROAD_CODE_RE.match(titolo.strip())
        if m:
            return m.group(1).upper()
        return titolo.strip() or None

    @staticmethod
    def _map_management_type(evento: str) -> str | None:
        if not evento:
            return None
        for pattern, management in _EVENT_TO_MANAGEMENT.items():
            if pattern in evento:
                return management
        return evento

    @staticmethod
    def _extract_cause(dettaglio: str, evento: str = "") -> str | None:
        m = _CAUSE_RE.search(dettaglio)
        if m:
            keyword = m.group(1).lower()
            for cause_keyword, cause_type in _CAUSE_KEYWORD_MAP.items():
                if cause_keyword in keyword:
                    return cause_type
        lower = dettaglio.lower()
        if "lavori" in lower:
            return "roadMaintenance"
        if "code" in evento or "traffico" in evento:
            return "abnormalTraffic"
        return None

    @staticmethod
    def _parse_publication_date(date_str: str | None) -> datetime | None:
        if not date_str:
            return None
        try:
            naive = datetime.strptime(date_str.strip(), "%d/%m/%Y %H:%M")
            return naive.replace(tzinfo=_ROME_TZ)
        except ValueError:
            return None

    @staticmethod
    def _parse_italian_date(
        time_str: str, day: str, month_name: str, year: str
    ) -> datetime | None:
        month = _ITALIAN_MONTHS.get(month_name.lower())
        if month is None:
            return None
        try:
            parts = time_str.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            return datetime(int(year), month, int(day), hour, minute, tzinfo=_ROME_TZ)
        except (ValueError, IndexError):
            return None

    @classmethod
    def _parse_detail_dates(
        cls, dettaglio: str
    ) -> tuple[datetime | None, datetime | None]:
        start_time = None
        end_time = None

        m_start = _START_DATE_RE.search(dettaglio)
        if m_start:
            start_time = cls._parse_italian_date(
                m_start.group(1),
                m_start.group(2),
                m_start.group(3),
                m_start.group(4),
            )

        m_end = _END_DATE_RE.search(dettaglio)
        if m_end:
            end_time = cls._parse_italian_date(
                m_end.group(1),
                m_end.group(2),
                m_end.group(3),
                m_end.group(4),
            )

        return start_time, end_time

    @staticmethod
    def _parse_tratto(tratto: str) -> tuple[str | None, str | None]:
        if not tratto:
            return None, None
        m = _TRATTO_TWO_RE.match(tratto)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        m = _TRATTO_SINGLE_RE.match(tratto)
        if m:
            return m.group(1).strip(), None
        return None, None

    @staticmethod
    def _extract_location(
        event: dict[str, Any], coord_key: str
    ) -> LocationPoint | None:
        percorso: dict[str, Any] | None = event.get("percorsoTO")
        if not percorso:
            return None
        coords: dict[str, Any] | None = percorso.get(coord_key)
        if not coords:
            return None
        lon = coords.get("coordinateMappaCX")
        lat = coords.get("coordinateMappaCY")
        if lon is None or lat is None:
            return None
        return LocationPoint(latitude=lat, longitude=lon)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _save_alerts(alerts: list[TruckDashboardAlert], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = [a.model_dump(mode="json") for a in alerts]
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Saved {len(alerts)} CCISS alerts → {path}")
