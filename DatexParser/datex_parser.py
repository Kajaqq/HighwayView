"""DATEX II parser for traffic situation data.

Parses Spanish/Dutch DATEX II v3 and French DATEX II v2 SituationPublication
XML feeds. Every ``situationRecord`` becomes a
:class:`TruckDashboardAlert`, and the parser exposes three filtering
methods for downstream consumers (road, admin-area, and GPS-radius
queries).

Example::

    parser = DatexParser(downloader=GenericDownloader())
    alerts = await parser.get_parsed_data()
    nearby = parser.get_alerts_near(lat=38.98, lon=-5.53, radius=100)
"""

from __future__ import annotations

import gzip
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from lxml import etree

from Downloaders.base_downloader import GenericDownloader, HTTPError
from Parsers.base_parser import BaseParser
from tools.utils import haversine_km
from .datex_models import (
    NON_TRUCK_VEHICLE_TYPES,
    LocationPoint,
    TruckDashboardAlert,
)

_DATEX_V2_NAMESPACE_MARKER = "/schema/2/"
_DUTCH_NAMESPACE_MARKERS = ("nlExtensions", "nlxExtensions")
_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_ROAD_NUMBER_PADDING_RE = re.compile(r"([A-Za-z])0+(?=\d)")
_FRENCH_PR_RE = re.compile(r"^\d{2}PR(\d+)")
_DUTCH_ROAD_RE = re.compile(r"\b([AN])[-\s]?(\d{1,3})\b", re.IGNORECASE)
_DUTCH_VILD_DB = Path(__file__).parent / "data" / "vild_6.13.A.sqlite"
_DATEX_HTTP_RETRIES = 3

_FRENCH_DETAIL_TAGS: tuple[str, ...] = (
    "d2:accidentType",
    "d2:abnormalTrafficType",
    "d2:environmentalObstructionType",
    "d2:vehicleObstructionType",
    "d2:weatherRelatedRoadConditionType",
    "d2:roadMaintenanceType",
    "d2:roadsideServiceDisruptionType",
    "d2:roadOrCarriagewayOrLaneManagementType",
    "d2:networkManagementType",
    "d2:drivingConditionType",
)

_FRENCH_RECORD_CAUSE_TYPES: dict[str, str] = {
    "Accident": "accident",
    "AbnormalTraffic": "abnormalTraffic",
    "EnvironmentalObstruction": "poorEnvironmentConditions",
    "RoadMaintenance": "roadMaintenance",
    "RoadOrCarriagewayOrLaneManagement": "roadManagement",
    "RoadsideServiceDisruption": "roadsideServiceDisruption",
    "VehicleObstruction": "obstruction",
    "WeatherRelatedRoadConditions": "poorWeatherConditions",
}

_DUTCH_DETAIL_TAGS: tuple[str, ...] = (
    "sit:accidentType",
    "sit:abnormalTrafficType",
    "sit:environmentalObstructionType",
    "sit:generalNetworkManagementType",
    "sit:generalObstructionType",
    "sit:roadOrCarriagewayOrLaneManagementType",
    "sit:reroutingManagementType",
    "sit:speedManagementType",
    "sit:vehicleObstructionType",
    "sit:weatherRelatedRoadConditionType",
)

_DUTCH_RECORD_CAUSE_TYPES: dict[str, str] = {
    "AbnormalTraffic": "abnormalTraffic",
    "Accident": "accident",
    "EnvironmentalObstruction": "poorEnvironmentConditions",
    "GeneralNetworkManagement": "roadOrCarriagewayOrLaneManagement",
    "GeneralObstruction": "obstruction",
    "RoadOrCarriagewayOrLaneManagement": "roadOrCarriagewayOrLaneManagement",
    "ReroutingManagement": "roadOrCarriagewayOrLaneManagement",
    "SpeedManagement": "roadOrCarriagewayOrLaneManagement",
    "VehicleObstruction": "vehicleObstruction",
    "WeatherRelatedRoadConditions": "poorWeatherConditions",
}


@dataclass(frozen=True, slots=True)
class _VildLocation:
    loc_nr: int
    road_number: str | None
    road_name: str | None
    road_label: str | None
    first_name: str | None
    second_name: str | None
    place_name: str | None
    area_name: str | None
    loc_type: str | None
    loc_description: str | None
    area_ref: int | None
    line_ref: int | None
    km_start_pos: float | None
    km_end_pos: float | None
    km_start_neg: float | None
    km_end_neg: float | None


class DatexParser(BaseParser):
    """Parser for DATEX II SituationPublication XML.

    After calling :meth:`get_parsed_data`, the parsed alerts are stored
    internally and can be queried via :meth:`filter_by_road`,
    :meth:`filter_by_admin`, and :meth:`filter_by_location`.

    Args:
        downloader: HTTP downloader instance.  Defaults to ``None``.
        truck_only: When ``True`` (the default), alerts whose
            ``vehicleType`` is exclusively non-truck (e.g. ``bicycle``)
            are **excluded**.  Set to ``False`` to include everything.
        datex_url: Feed URL used by :meth:`get_parsed_data`.
        country_code: Default country code before parsing. French v2 feeds
            update this from ``supplierIdentification``.
    """

    def __init__(
        self,
        datex_url: str,
        downloader: GenericDownloader | None = None,
        truck_only: bool = True,
        country_code: str = "ES",
    ) -> None:
        super().__init__(downloader)
        self.truck_only = truck_only
        self.datex_url = datex_url
        self._country = country_code
        self._alerts: list[TruckDashboardAlert] = []
        self._dutch_vild_locations: dict[int, _VildLocation] | None = None

    @property
    def country(self) -> str:
        """Returns the two-letter country code for the latest parsed feed."""
        return self._country

    @property
    def alerts(self) -> list[TruckDashboardAlert]:
        """The most recently parsed list of alerts."""
        return self._alerts

    # ------------------------------------------------------------------
    # Core parsing
    # ------------------------------------------------------------------

    async def parse(self, raw_data: str) -> list[TruckDashboardAlert]:
        """Parse raw DATEX II XML into a list of alerts.

        Args:
            raw_data: The XML document as a UTF-8 string.

        Returns:
            A list of :class:`TruckDashboardAlert` instances.
        """
        xml_parser = etree.XMLParser(resolve_entities=False, no_network=True)
        root = etree.fromstring(raw_data.encode("utf-8"), parser=xml_parser)
        nsmap = self._build_nsmap(root)

        if self._is_datex_v2(nsmap):
            alerts = self._parse_french_v2(root, nsmap)
            country = "FR"
        elif self._country == "NL" or self._is_dutch_v3(nsmap):
            alerts = self._parse_dutch_v3(root, nsmap)
            country = "NL"
        else:
            alerts = self._parse_spanish_v3(root, nsmap)
            country = "ES"

        self._alerts = alerts
        print(f"[{country}] Parsed {len(alerts)} DATEX II alerts.")
        return alerts

    def _parse_spanish_v3(
        self,
        root: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> list[TruckDashboardAlert]:
        """Parse Spain's DATEX II v3 SituationPublication payload."""
        self._country = "ES"
        alerts: list[TruckDashboardAlert] = []

        for situation in root.findall(".//sit:situation", nsmap):
            situation_id = situation.get("id", "")
            overall_severity = self._text(situation, "sit:overallSeverity", nsmap)

            for record in situation.findall("sit:situationRecord", nsmap):
                alert = self._parse_record(
                    record, situation_id, overall_severity, nsmap
                )
                if self.truck_only and self._is_non_truck_only(alert):
                    continue
                alerts.append(alert)

        return alerts

    async def get_parsed_data(
        self,
        output_file: str | Path | None = None,
        output_folder: str | Path | None = None,
    ) -> list[TruckDashboardAlert]:
        """Download, parse, and optionally save DATEX II alerts.

        Args:
            output_file: Explicit file path to save JSON output.
            output_folder: Folder — file will be named
                ``datex_alerts.json``.

        Returns:
            The list of parsed alerts.
        """
        if self.downloader is None:
            raise RuntimeError(
                "DatexParser requires a downloader to call get_parsed_data()"
            )
        for retry in range(_DATEX_HTTP_RETRIES + 1):
            try:
                raw_data = await self._download_raw_data()
                break
            except HTTPError:
                if retry == _DATEX_HTTP_RETRIES:
                    raise
        else:
            raise RuntimeError("DATEX download failed without an HTTP error")

        alerts = await self.parse(raw_data)

        if output_file:
            self.save_alerts(alerts, Path(output_file))
        elif output_folder:
            self.save_alerts(alerts, Path(output_folder) / "datex_alerts.json")

        return alerts

    async def _download_raw_data(self) -> str:
        if self.downloader is None:
            raise RuntimeError("DatexParser requires a downloader to download data")

        if self.datex_url.endswith(".gz"):
            raw_bytes = await self.downloader.download_bytes(self.datex_url)
            if raw_bytes.startswith(b"\x1f\x8b"):
                raw_bytes = gzip.decompress(raw_bytes)
            return raw_bytes.decode("utf-8")

        return await self.downloader.download(self.datex_url)

    # ------------------------------------------------------------------
    # Filtering 
    # ------------------------------------------------------------------

    def filter_by_road(self, road: str) -> list[TruckDashboardAlert]:
        """Return alerts whose ``road_name`` matches *road* (case-sensitive).

        Args:
            road: Road code to match exactly (e.g. ``"AP-7"``).

        Returns:
            Filtered list of alerts.
        """
        return [a for a in self._alerts if a.road_name and road == a.road_name]

    def filter_by_admin(
        self,
        community: str | None = None,
        province: str | None = None,
        municipality: str | None = None,
    ) -> list[TruckDashboardAlert]:
        """Return alerts matching administrative metadata (case-insensitive).

        Checks **both** ``location_from`` and ``location_to`` so that
        cross-province incidents are not missed.

        Args:
            community: Autonomous Community to match.
            province: Province to match.
            municipality: Municipality to match.

        Returns:
            Filtered list of alerts.
        """

        def _matches_point(point: LocationPoint | None) -> bool:
            if point is None:
                return False
            if community and (
                not point.community or community.lower() not in point.community.lower()
            ):
                return False
            if province and (
                not point.province or province.lower() not in point.province.lower()
            ):
                return False
            if municipality and (
                not point.municipality
                or municipality.lower() not in point.municipality.lower()
            ):
                return False
            return True

        return [
            a
            for a in self._alerts
            if _matches_point(a.location_from) or _matches_point(a.location_to)
        ]

    def filter_by_location(
        self, lat: float, lon: float, radius_km: float
    ) -> list[TruckDashboardAlert]:
        """Return alerts within *radius_km* of a GPS coordinate.

        Uses the Haversine formula.  Checks distance to **both**
        ``location_from`` and ``location_to`` coordinates.

        Args:
            lat: Latitude of the query center.
            lon: Longitude of the query center.
            radius_km: Maximum distance in kilometers.

        Returns:
            Filtered list of alerts.
        """

        def _within(point: LocationPoint | None) -> bool:
            if point is None or point.latitude is None or point.longitude is None:
                return False
            return haversine_km(lat, lon, point.latitude, point.longitude) <= radius_km

        return [
            a
            for a in self._alerts
            if _within(a.location_from) or _within(a.location_to)
        ]

    def get_alerts_near(
        self, lat: float, lon: float, radius: float
    ) -> list[TruckDashboardAlert]:
        """Alias for :meth:`filter_by_location`.

        Args:
            lat: Center latitude.
            lon: Center longitude.
            radius: Radius in kilometers.

        Returns:
            Alerts within the radius.
        """
        return self.filter_by_location(lat, lon, radius)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def save_alerts(alerts: list[TruckDashboardAlert], path: Path) -> None:
        """Serialize alerts to a JSON file.

        Args:
            alerts: The alert list to save.
            path: Destination file path.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        data = [a.model_dump(mode="json") for a in alerts]
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Saved {len(alerts)} alerts → {path}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_nsmap(root: etree.ElementBase) -> dict[str, str]:
        """Build a prefix → URI namespace map from the XML root.

        Args:
            root: The lxml root element.

        Returns:
            Namespace dictionary suitable for ``findall`` / ``find``.
        """
        nsmap: dict[str, str] = {}
        for element in root.iter():
            for prefix, uri in element.nsmap.items():
                if prefix is None:
                    if _DATEX_V2_NAMESPACE_MARKER in uri:
                        nsmap.setdefault("d2", uri)
                    continue
                nsmap.setdefault(prefix, uri)

        for uri in tuple(nsmap.values()):
            if _DATEX_V2_NAMESPACE_MARKER in uri:
                nsmap.setdefault("d2", uri)
                break

        return nsmap

    @staticmethod
    def _is_datex_v2(nsmap: dict[str, str]) -> bool:
        """Return ``True`` when the document uses the monolithic DATEX II v2 ns."""
        return any(_DATEX_V2_NAMESPACE_MARKER in uri for uri in nsmap.values())

    @staticmethod
    def _is_dutch_v3(nsmap: dict[str, str]) -> bool:
        return any(
            marker in uri
            for uri in nsmap.values()
            for marker in _DUTCH_NAMESPACE_MARKERS
        )

    @staticmethod
    def _text(
        element: etree.ElementBase,
        xpath: str,
        nsmap: dict[str, str],
    ) -> str | None:
        """Safe text extraction via XPath.

        Args:
            element: Parent element.
            xpath: XPath expression.
            nsmap: Namespace map.

        Returns:
            The text content, or ``None`` if the node is missing.
        """
        node = element.find(xpath, nsmap)
        return node.text if node is not None else None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value)

    @staticmethod
    def _float_or_none(value: str | None) -> float | None:
        """Convert a string to ``float`` when possible."""
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _bool_or_none(value: str | None) -> bool | None:
        """Convert common XML boolean text to ``bool``."""
        if value is None:
            return None
        return value.strip().lower() == "true"

    @staticmethod
    def _strip_type_prefix(value: str | None) -> str | None:
        """Remove XML namespace prefixes from an ``xsi:type`` value."""
        if not value:
            return None
        return value.rsplit(":", maxsplit=1)[-1]

    def _first_text(
        self,
        element: etree.ElementBase,
        paths: tuple[str, ...],
        nsmap: dict[str, str],
    ) -> str | None:
        """Return text from the first matching XPath in *paths*."""
        for path in paths:
            value = self._text(element, path, nsmap)
            if value:
                return value
        return None

    @staticmethod
    def _merge_location_points(
        base: LocationPoint | None,
        update: LocationPoint | None,
    ) -> LocationPoint | None:
        if base is None:
            return update
        if update is None:
            return base
        values = {
            field: value
            for field, value in update.model_dump().items()
            if value is not None
        }
        return base.model_copy(update=values)

    # ------------------------------------------------------------------
    # French DATEX II v2 helpers
    # ------------------------------------------------------------------

    def _parse_french_v2(
        self,
        root: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> list[TruckDashboardAlert]:
        """Parse French DATEX II v2 SituationPublication XML.

        French feeds are commonly wrapped in SOAP and use a monolithic v2
        namespace. Searching from the document root handles both SOAP and raw
        ``d2LogicalModel`` payloads.
        """
        country = self._text(root, ".//d2:supplierIdentification/d2:country", nsmap)
        self._country = (country or "FR").upper()

        alerts: list[TruckDashboardAlert] = []
        for situation in root.findall(".//d2:situation", nsmap):
            situation_id = situation.get("id", "")
            overall_severity = self._text(situation, "d2:overallSeverity", nsmap)

            for record in situation.findall("d2:situationRecord", nsmap):
                alert = self._parse_french_v2_record(
                    record,
                    situation_id,
                    overall_severity,
                    nsmap,
                )
                if self.truck_only and self._is_non_truck_only(alert):
                    continue
                alerts.append(alert)

        return alerts

    def _parse_french_v2_record(
        self,
        record: etree.ElementBase,
        situation_id: str,
        overall_severity: str | None,
        nsmap: dict[str, str],
    ) -> TruckDashboardAlert:
        """Parse a French DATEX II v2 ``situationRecord``."""
        record_id = record.get("id", "")
        record_type = self._strip_type_prefix(record.get(f"{{{_XSI_NS}}}type"))
        detailed_cause_type = self._first_text(record, _FRENCH_DETAIL_TAGS, nsmap)
        cause_type = _FRENCH_RECORD_CAUSE_TYPES.get(
            record_type or "",
            record_type or detailed_cause_type,
        )

        management_type = self._text(
            record,
            "d2:roadOrCarriagewayOrLaneManagementType",
            nsmap,
        )
        if detailed_cause_type is None:
            detailed_cause_type = management_type or record_type

        location_from, location_to, direction = self._parse_french_group_of_locations(
            record,
            nsmap,
        )

        road_name = self._extract_french_road_name(record, nsmap)
        comments = self._extract_french_public_comments(record, nsmap)
        road_destination = self._extract_french_direction_comment(comments)

        safety_related_message = self._bool_or_none(
            self._text(
                record,
                "d2:situationRecordExtension/"
                "d2:situationRecordExtendedApproved/"
                "d2:safetyRelatedMessage",
                nsmap,
            )
        )

        return TruckDashboardAlert(
            situation_id=situation_id,
            record_id=record_id,
            creation_time=self._parse_datetime(
                self._text(record, "d2:situationRecordCreationTime", nsmap)
            ),
            version_time=self._parse_datetime(
                self._text(record, "d2:situationRecordVersionTime", nsmap)
            ),
            severity=self._text(record, "d2:severity", nsmap) or overall_severity,
            start_time=self._parse_datetime(
                self._text(
                    record,
                    "d2:validity/d2:validityTimeSpecification/d2:overallStartTime",
                    nsmap,
                )
            ),
            end_time=self._parse_datetime(
                self._text(
                    record,
                    "d2:validity/d2:validityTimeSpecification/d2:overallEndTime",
                    nsmap,
                )
            ),
            management_type=management_type,
            vehicle_type=self._text(
                record,
                "d2:forVehiclesWithCharacteristicsOf/d2:vehicleType",
                nsmap,
            ),
            cause_type=cause_type,
            detailed_cause_type=detailed_cause_type,
            road_name=road_name,
            road_destination=road_destination,
            direction=direction,
            carriageway=self._text(
                record,
                "d2:groupOfLocations/"
                "d2:supplementaryPositionalDescription/"
                "d2:affectedCarriagewayAndLanes/"
                "d2:carriageway",
                nsmap,
            ),
            lane_usage=self._text(
                record,
                "d2:groupOfLocations/"
                "d2:supplementaryPositionalDescription/"
                "d2:affectedCarriagewayAndLanes/"
                "d2:lane",
                nsmap,
            ),
            location_from=location_from,
            location_to=location_to,
            public_comments=comments,
            safety_related_message=safety_related_message,
        )

    def _parse_french_group_of_locations(
        self,
        record: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        """Parse French v2 ``groupOfLocations`` into model points."""
        group = record.find("d2:groupOfLocations", nsmap)
        if group is None:
            return None, None, None

        linear = group.find("d2:tpegLinearLocation", nsmap)
        if linear is not None:
            location_from, location_to = self._parse_french_linear_points(
                group,
                linear,
                nsmap,
            )
            direction = self._text(linear, "d2:tpegDirection", nsmap)
            if direction is None:
                direction = self._text(
                    group,
                    "d2:alertCLinear/d2:alertCDirection/d2:alertCDirectionCoded",
                    nsmap,
                )
            return location_from, location_to, direction

        point_location = group.find("d2:tpegPointLocation", nsmap)
        if point_location is not None:
            point = point_location.find("d2:point", nsmap)
            location = (
                self._parse_french_tpeg_point(point, nsmap)
                if point is not None
                else LocationPoint()
            )
            location = self._apply_french_pr_reference(
                location,
                group.find(
                    "d2:pointAlongLinearElement/d2:distanceAlongLinearElement",
                    nsmap,
                ),
                nsmap,
            )
            location = self._apply_french_alertc_location(
                location,
                group.find(
                    "d2:alertCPoint/d2:alertCMethod4PrimaryPointLocation",
                    nsmap,
                ),
                nsmap,
            )
            direction = self._text(point_location, "d2:tpegDirection", nsmap)
            if direction is None:
                direction = self._text(
                    group,
                    "d2:alertCPoint/d2:alertCDirection/d2:alertCDirectionCoded",
                    nsmap,
                )
            return location, None, direction

        return None, None, None

    def _parse_french_linear_points(
        self,
        group: etree.ElementBase,
        linear: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None]:
        """Parse a French v2 TPEG linear location with PR/Alert-C metadata."""
        from_el = linear.find("d2:from", nsmap)
        to_el = linear.find("d2:to", nsmap)

        location_from = (
            self._parse_french_tpeg_point(from_el, nsmap)
            if from_el is not None
            else LocationPoint()
        )
        location_to = (
            self._parse_french_tpeg_point(to_el, nsmap)
            if to_el is not None
            else LocationPoint()
        )

        location_from = self._apply_french_pr_reference(
            location_from,
            group.find("d2:linearWithinLinearElement/d2:fromPoint", nsmap),
            nsmap,
        )
        location_to = self._apply_french_pr_reference(
            location_to,
            group.find("d2:linearWithinLinearElement/d2:toPoint", nsmap),
            nsmap,
        )
        location_from = self._apply_french_alertc_location(
            location_from,
            group.find(
                "d2:alertCLinear/d2:alertCMethod4SecondaryPointLocation",
                nsmap,
            ),
            nsmap,
        )
        location_to = self._apply_french_alertc_location(
            location_to,
            group.find("d2:alertCLinear/d2:alertCMethod4PrimaryPointLocation", nsmap),
            nsmap,
        )
        return location_from, location_to

    def _parse_french_tpeg_point(
        self,
        point_el: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> LocationPoint:
        """Extract coordinates and town name from a French v2 TPEG point."""
        lat = self._text(point_el, "d2:pointCoordinates/d2:latitude", nsmap)
        lon = self._text(point_el, "d2:pointCoordinates/d2:longitude", nsmap)

        return LocationPoint(
            latitude=self._float_or_none(lat),
            longitude=self._float_or_none(lon),
            municipality=self._extract_french_tpeg_name(point_el, "townName", nsmap),
        )

    @staticmethod
    def _pr_marker_to_km_point(
        reference_marker: str | None,
        offset_m: float | None,
    ) -> float | None:
        """Convert a French PR marker and meter offset to a km point.

        French PR markers follow the format ``{dept}PR{km}{side}``
        (e.g. ``09PR54U`` = department 09, km 54, both sides).  The
        digit group after ``PR`` is the integer km marker; *offset_m*
        (which may be negative) is added as a fractional km.

        Args:
            reference_marker: The raw PR identifier string.
            offset_m: Signed distance in meters from the marker.

        Returns:
            The computed km point, or ``None`` when the marker cannot
            be parsed.
        """
        if not reference_marker:
            return None
        m = _FRENCH_PR_RE.match(reference_marker)
        if m is None:
            return None
        km = float(m.group(1))
        if offset_m is not None:
            km += offset_m / 1000.0
        return round(km, 3)

    def _apply_french_pr_reference(
        self,
        location: LocationPoint | None,
        reference_el: etree.ElementBase | None,
        nsmap: dict[str, str],
    ) -> LocationPoint | None:
        """Add French PR reference-marker metadata to a location point."""
        if reference_el is None:
            return location

        location = location or LocationPoint()
        reference_marker = self._text(reference_el, ".//d2:referentIdentifier", nsmap)
        offset_m = self._float_or_none(
            self._text(reference_el, "d2:distanceAlong", nsmap)
        )
        km_point = self._pr_marker_to_km_point(reference_marker, offset_m)
        return location.model_copy(
            update={
                "reference_marker": reference_marker or location.reference_marker,
                "offset_m": offset_m if offset_m is not None else location.offset_m,
                "km_point": km_point if km_point is not None else location.km_point,
            }
        )

    def _apply_french_alertc_location(
        self,
        location: LocationPoint | None,
        alertc_el: etree.ElementBase | None,
        nsmap: dict[str, str],
    ) -> LocationPoint | None:
        """Add Alert-C/TMC metadata to a location point."""
        if alertc_el is None:
            return location

        location = location or LocationPoint()
        location_id = self._text(
            alertc_el, "d2:alertCLocation/d2:specificLocation", nsmap
        )
        location_name = self._text(
            alertc_el,
            "d2:alertCLocation/d2:alertCLocationName/d2:values/d2:value",
            nsmap,
        )
        offset_m = self._float_or_none(
            self._text(alertc_el, "d2:offsetDistance/d2:offsetDistance", nsmap)
        )
        return location.model_copy(
            update={
                "alertc_location_id": location_id or location.alertc_location_id,
                "alertc_location_name": location_name or location.alertc_location_name,
                "offset_m": location.offset_m
                if location.offset_m is not None
                else offset_m,
            }
        )

    def _extract_french_road_name(
        self,
        record: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> str | None:
        """Extract the best French road designation."""
        for name_el in record.findall(".//d2:name", nsmap):
            if (
                self._text(name_el, "d2:tpegOtherPointDescriptorType", nsmap)
                != "linkName"
            ):
                continue
            road_name = self._text(name_el, "d2:descriptor/d2:values/d2:value", nsmap)
            if road_name:
                return road_name

        road_number = self._text(record, ".//d2:roadNumber", nsmap)
        if road_number is None:
            return None
        return _ROAD_NUMBER_PADDING_RE.sub(r"\1", road_number)

    def _extract_french_tpeg_name(
        self,
        point_el: etree.ElementBase,
        descriptor_type: str,
        nsmap: dict[str, str],
    ) -> str | None:
        """Extract a TPEG descriptor value from a French v2 point."""
        for name_el in point_el.findall("d2:name", nsmap):
            if (
                self._text(name_el, "d2:tpegOtherPointDescriptorType", nsmap)
                == descriptor_type
            ):
                return self._text(name_el, "d2:descriptor/d2:values/d2:value", nsmap)
        return None

    def _extract_french_public_comments(
        self,
        record: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> list[str]:
        """Return cleaned French ``generalPublicComment`` values."""
        comments: list[str] = []
        for comment in record.findall("d2:generalPublicComment", nsmap):
            value = self._text(comment, "d2:comment/d2:values/d2:value", nsmap)
            if value:
                comments.append(" ".join(value.split()))
        return comments

    @staticmethod
    def _extract_french_direction_comment(comments: list[str]) -> str | None:
        """Return the common French ``De X vers Y`` direction comment."""
        for comment in comments:
            lower = comment.lower()
            if lower.startswith("de ") and " vers " in lower:
                return comment
        return None

    # ------------------------------------------------------------------
    # Dutch DATEX II v3 helpers
    # ------------------------------------------------------------------

    def _parse_dutch_v3(
        self,
        root: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> list[TruckDashboardAlert]:
        """Parse NDW DATEX II v3 SituationPublication payloads."""
        self._country = "NL"
        alerts: list[TruckDashboardAlert] = []

        for situation in root.findall(".//sit:situation", nsmap):
            situation_id = situation.get("id", "")
            overall_severity = self._text(situation, "sit:overallSeverity", nsmap)

            for record in situation.findall("sit:situationRecord", nsmap):
                alert = self._parse_dutch_v3_record(
                    record,
                    situation_id,
                    overall_severity,
                    nsmap,
                )
                if self.truck_only and self._is_non_truck_only(alert):
                    continue
                alerts.append(alert)

        return alerts

    def _parse_dutch_v3_record(
        self,
        record: etree.ElementBase,
        situation_id: str,
        overall_severity: str | None,
        nsmap: dict[str, str],
    ) -> TruckDashboardAlert:
        record_id = record.get("id", "")
        record_type = self._strip_type_prefix(record.get(f"{{{_XSI_NS}}}type"))
        comments = self._extract_dutch_public_comments(record, nsmap)
        detailed_cause_type = self._first_text(record, _DUTCH_DETAIL_TAGS, nsmap)
        cause_type = (
            self._text(record, "sit:cause/sit:causeType", nsmap)
            or _DUTCH_RECORD_CAUSE_TYPES.get(record_type or "")
            or record_type
        )
        management_type = self._first_text(
            record,
            (
                "sit:roadOrCarriagewayOrLaneManagementType",
                "sit:speedManagementType",
                "sit:reroutingManagementType",
                "sit:generalNetworkManagementType",
            ),
            nsmap,
        )

        loc_ref = record.find("sit:locationReference", nsmap)
        location_from: LocationPoint | None = None
        location_to: LocationPoint | None = None
        direction: str | None = None
        carriageway: str | None = None
        lane_usage: str | None = None

        if loc_ref is not None:
            location_from, location_to, direction = self._parse_dutch_location(
                loc_ref,
                nsmap,
            )
            carriageway = self._text(
                loc_ref,
                ".//loc:supplementaryPositionalDescription/"
                "loc:carriageway/loc:carriageway",
                nsmap,
            )
            lane_usage = self._text(
                loc_ref,
                ".//loc:supplementaryPositionalDescription/"
                "loc:carriageway/loc:lane/loc:laneUsage",
                nsmap,
            )

        road_name = self._extract_dutch_location_road_name(
            location_from,
            location_to,
        ) or self._extract_dutch_road_name(record_id, comments)
        road_destination = self._extract_dutch_location_name(
            location_from,
            location_to,
        )

        return TruckDashboardAlert(
            situation_id=situation_id,
            record_id=record_id,
            creation_time=self._parse_datetime(
                self._text(record, "sit:situationRecordCreationTime", nsmap)
            ),
            version_time=self._parse_datetime(
                self._text(record, "sit:situationRecordVersionTime", nsmap)
            ),
            severity=self._text(record, "sit:severity", nsmap) or overall_severity,
            start_time=self._parse_datetime(
                self._text(
                    record,
                    "sit:validity/com:validityTimeSpecification/com:overallStartTime",
                    nsmap,
                )
            ),
            end_time=self._parse_datetime(
                self._text(
                    record,
                    "sit:validity/com:validityTimeSpecification/com:overallEndTime",
                    nsmap,
                )
            ),
            management_type=management_type,
            vehicle_type=self._text(
                record,
                "sit:forVehiclesWithCharacteristicsOf/com:vehicleType",
                nsmap,
            ),
            cause_type=cause_type,
            detailed_cause_type=detailed_cause_type,
            road_name=road_name,
            road_destination=road_destination,
            direction=direction,
            carriageway=carriageway,
            lane_usage=lane_usage,
            location_from=location_from,
            location_to=location_to,
            public_comments=comments,
            safety_related_message=self._bool_or_none(
                self._text(record, "sit:safetyRelatedMessage", nsmap)
            ),
        )

    def _parse_dutch_location(
        self,
        loc_ref: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        loc_type = self._strip_type_prefix(loc_ref.get(f"{{{_XSI_NS}}}type"))

        if loc_type == "PointLocation":
            point, direction = self._parse_dutch_point_location(loc_ref, nsmap)
            return point, None, direction

        if loc_type == "ItineraryByIndexedLocations":
            return self._parse_dutch_itinerary_location(loc_ref, nsmap)

        if loc_type in {"LinearLocation", "SingleRoadLinearLocation"}:
            return self._parse_dutch_linear_location(loc_ref, nsmap)

        return None, None, None

    def _parse_dutch_point_location(
        self,
        loc_ref: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, str | None]:
        point_by_coordinates = loc_ref.find("loc:pointByCoordinates", nsmap)
        location = (
            self._parse_dutch_point_by_coordinates(point_by_coordinates, nsmap)
            if point_by_coordinates is not None
            else LocationPoint()
        )
        location = self._merge_location_points(
            location,
            self._parse_dutch_alertc_point(
                loc_ref.find("loc:alertCPoint", nsmap),
                nsmap,
                self._text(
                    loc_ref,
                    "loc:alertCPoint/loc:alertCDirection/loc:alertCDirectionCoded",
                    nsmap,
                ),
            ),
        )
        direction = self._text(
            loc_ref,
            "loc:alertCPoint/loc:alertCDirection/loc:alertCDirectionCoded",
            nsmap,
        )
        return location, direction

    def _parse_dutch_itinerary_location(
        self,
        loc_ref: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        location_from: LocationPoint | None = None
        location_to: LocationPoint | None = None
        direction: str | None = None

        for location in loc_ref.findall(
            "loc:locationContainedInItinerary/loc:location",
            nsmap,
        ):
            current_from, current_to, current_direction = (
                self._parse_dutch_linear_location(location, nsmap)
            )
            location_from = self._merge_location_points(location_from, current_from)
            location_to = self._merge_location_points(location_to, current_to)
            direction = direction or current_direction

        return location_from, location_to, direction

    def _parse_dutch_linear_location(
        self,
        location: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        location_from, location_to = self._parse_dutch_gml_line_points(
            location,
            nsmap,
        )
        alertc_from, alertc_to, direction = self._parse_dutch_alertc_linear(
            location,
            nsmap,
        )
        return (
            self._merge_location_points(location_from, alertc_from),
            self._merge_location_points(location_to, alertc_to),
            direction,
        )

    def _parse_dutch_point_by_coordinates(
        self,
        point_el: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> LocationPoint:
        lat = self._text(point_el, "loc:pointCoordinates/loc:latitude", nsmap)
        lon = self._text(point_el, "loc:pointCoordinates/loc:longitude", nsmap)
        return LocationPoint(
            latitude=self._float_or_none(lat),
            longitude=self._float_or_none(lon),
        )

    def _parse_dutch_gml_line_points(
        self,
        location: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None]:
        pos_list = self._text(location, "loc:gmlLineString/loc:posList", nsmap)
        if not pos_list:
            return None, None

        try:
            values = [float(value) for value in pos_list.split()]
        except ValueError:
            return None, None

        pairs = list(zip(values[0::2], values[1::2], strict=False))
        if not pairs:
            return None, None

        first_lat, first_lon = pairs[0]
        last_lat, last_lon = pairs[-1]
        return (
            LocationPoint(latitude=first_lat, longitude=first_lon),
            LocationPoint(latitude=last_lat, longitude=last_lon),
        )

    def _parse_dutch_alertc_point(
        self,
        alertc_el: etree.ElementBase | None,
        nsmap: dict[str, str],
        direction: str | None,
    ) -> LocationPoint | None:
        if alertc_el is None:
            return None
        return self._parse_dutch_alertc_method_location(
            alertc_el.find("loc:alertCMethod4PrimaryPointLocation", nsmap),
            nsmap,
            direction,
        )

    def _parse_dutch_alertc_linear(
        self,
        location: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        alertc_linear = location.find("loc:alertCLinear", nsmap)
        if alertc_linear is None:
            return None, None, None

        direction = self._text(
            alertc_linear,
            "loc:alertCDirection/loc:alertCDirectionCoded",
            nsmap,
        )
        location_from = self._parse_dutch_alertc_method_location(
            alertc_linear.find("loc:alertCMethod4SecondaryPointLocation", nsmap),
            nsmap,
            direction,
            "secondary",
        )
        location_to = self._parse_dutch_alertc_method_location(
            alertc_linear.find("loc:alertCMethod4PrimaryPointLocation", nsmap),
            nsmap,
            direction,
            "primary",
        )
        return location_from, location_to, direction

    def _parse_dutch_alertc_method_location(
        self,
        method_el: etree.ElementBase | None,
        nsmap: dict[str, str],
        direction: str | None = None,
        role: str = "point",
    ) -> LocationPoint | None:
        if method_el is None:
            return None

        location_id = self._text(
            method_el,
            "loc:alertCLocation/loc:specificLocation",
            nsmap,
        )
        offset_m = self._float_or_none(
            self._text(method_el, "loc:offsetDistance/loc:offsetDistance", nsmap)
        )
        return self._enrich_dutch_alertc_location(
            LocationPoint(
                alertc_location_id=location_id,
                reference_marker=location_id,
                offset_m=offset_m,
            ),
            direction,
            role,
        )

    def _extract_dutch_public_comments(
        self,
        record: etree.ElementBase,
        nsmap: dict[str, str],
    ) -> list[str]:
        comments: list[str] = []
        for comment in record.findall("sit:generalPublicComment", nsmap):
            value = self._text(comment, "sit:comment/com:values/com:value", nsmap)
            if value:
                comments.append(" ".join(value.split()))
        return comments

    @staticmethod
    def _extract_dutch_road_name(record_id: str, comments: list[str]) -> str | None:
        for value in (*comments, record_id):
            if match := _DUTCH_ROAD_RE.search(value):
                return f"{match.group(1).upper()}{match.group(2)}"
        return None

    def _get_dutch_vild_locations(self) -> dict[int, _VildLocation]:
        if self._dutch_vild_locations is not None:
            return self._dutch_vild_locations

        if not _DUTCH_VILD_DB.exists():
            self._dutch_vild_locations = {}
            return self._dutch_vild_locations

        query = """
            SELECT
                loc_nr,
                road_number,
                road_name,
                road_label,
                first_name,
                second_name,
                place_name,
                area_name,
                loc_type,
                loc_description,
                area_ref,
                line_ref,
                km_start_pos,
                km_end_pos,
                km_start_neg,
                km_end_neg
            FROM vild_locations
        """

        try:
            with sqlite3.connect(_DUTCH_VILD_DB) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute(query).fetchall()
        except sqlite3.Error:
            self._dutch_vild_locations = {}
            return self._dutch_vild_locations

        self._dutch_vild_locations = {
            int(row["loc_nr"]): _VildLocation(
                loc_nr=int(row["loc_nr"]),
                road_number=row["road_number"],
                road_name=row["road_name"],
                road_label=row["road_label"],
                first_name=row["first_name"],
                second_name=row["second_name"],
                place_name=row["place_name"],
                area_name=row["area_name"],
                loc_type=row["loc_type"],
                loc_description=row["loc_description"],
                area_ref=row["area_ref"],
                line_ref=row["line_ref"],
                km_start_pos=row["km_start_pos"],
                km_end_pos=row["km_end_pos"],
                km_start_neg=row["km_start_neg"],
                km_end_neg=row["km_end_neg"],
            )
            for row in rows
        }
        return self._dutch_vild_locations

    def _enrich_dutch_alertc_location(
        self,
        location: LocationPoint,
        direction: str | None = None,
        role: str = "point",
    ) -> LocationPoint:
        location_id = self._int_or_none(location.alertc_location_id)
        if location_id is None:
            return location

        locations = self._get_dutch_vild_locations()
        vild = locations.get(location_id)
        if vild is None:
            return location

        line = locations.get(vild.line_ref or 0)
        road_number = vild.road_number or (line.road_number if line else None)
        road_name = vild.road_name or (line.road_name if line else None)
        road_label = vild.road_label or (line.road_label if line else None)
        admin_update: dict[str, str] = {}
        visited: set[int] = set()
        area: _VildLocation | None = vild

        while area is not None and area.loc_nr not in visited:
            visited.add(area.loc_nr)
            name = area.place_name or area.first_name
            if name and area.loc_description == "Provincie":
                admin_update.setdefault("province", name)
            elif name and area.loc_description == "Gemeente":
                admin_update.setdefault("municipality", name)
            elif name and area.loc_description == "Plaats":
                admin_update.setdefault("community", name)
            area = locations.get(area.area_ref or 0)

        update = {
            "alertc_location_name": vild.place_name or vild.first_name,
            "alertc_road_number": road_number,
            "alertc_road_name": road_name,
            "alertc_road_label": road_label,
            "alertc_first_name": vild.first_name,
            "alertc_second_name": vild.second_name,
            "alertc_place_name": vild.place_name,
            "alertc_area_name": vild.area_name,
            "alertc_location_type": vild.loc_type,
            "alertc_location_description": vild.loc_description,
            "alertc_area_ref": vild.area_ref,
            "alertc_line_ref": vild.line_ref,
            "reference_marker": str(vild.loc_nr),
            "km_point": self._estimate_dutch_km_point(
                vild,
                location.offset_m,
                direction,
                role,
            ),
            **admin_update,
        }
        return location.model_copy(
            update={key: value for key, value in update.items() if value is not None}
        )

    @staticmethod
    def _estimate_dutch_km_point(
        vild: _VildLocation,
        offset_m: float | None,
        direction: str | None,
        role: str,
    ) -> float | None:
        km_range = DatexParser._select_dutch_vild_km_range(vild, direction)
        if km_range is None:
            return None

        start, end = km_range
        if offset_m is None:
            return round((start + end) / 2, 3)

        direction_sign = 1 if end >= start else -1
        if role == "primary":
            direction_sign *= -1

        return round(start + direction_sign * (offset_m / 1000), 3)

    @staticmethod
    def _select_dutch_vild_km_range(
        vild: _VildLocation,
        direction: str | None,
    ) -> tuple[float, float] | None:
        positive = DatexParser._valid_km_range(vild.km_start_pos, vild.km_end_pos)
        negative = DatexParser._valid_km_range(vild.km_start_neg, vild.km_end_neg)
        direction_key = (direction or "").lower()

        if direction_key == "negative" and negative is not None:
            return negative
        if direction_key == "positive" and positive is not None:
            return positive
        if positive is not None:
            return positive
        return negative

    @staticmethod
    def _valid_km_range(
        start: float | None,
        end: float | None,
    ) -> tuple[float, float] | None:
        if start is None or end is None:
            return None
        return start, end

    @staticmethod
    def _extract_dutch_location_road_name(
        *locations: LocationPoint | None,
    ) -> str | None:
        for location in locations:
            if location is not None and location.alertc_road_number:
                return location.alertc_road_number
        return None

    @staticmethod
    def _extract_dutch_location_name(
        *locations: LocationPoint | None,
    ) -> str | None:
        for location in locations:
            if location is None:
                continue
            if location.alertc_location_name:
                return location.alertc_location_name
            if location.municipality:
                return location.municipality
            if location.alertc_area_name:
                return location.alertc_area_name
        return None

    @staticmethod
    def _int_or_none(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _parse_record(
        self,
        record: etree.ElementBase,
        situation_id: str,
        overall_severity: str | None,
        nsmap: dict[str, str],
    ) -> TruckDashboardAlert:
        """Parse a single ``<sit:situationRecord>`` into a model.

        Args:
            record: The situationRecord XML element.
            situation_id: Parent situation ID.
            overall_severity: Severity from the parent situation.
            nsmap: Namespace map.

        Returns:
            A populated alert.
        """
        record_id = record.get("id", "")

        # --- Severity (record-level overrides situation-level) ---
        severity = self._text(record, "sit:severity", nsmap) or overall_severity

        # --- Timestamps ---
        creation_time = self._parse_datetime(
            self._text(record, "sit:situationRecordCreationTime", nsmap)
        )
        version_time = self._parse_datetime(
            self._text(record, "sit:situationRecordVersionTime", nsmap)
        )
        start_time = self._parse_datetime(
            self._text(
                record,
                "sit:validity/com:validityTimeSpecification/com:overallStartTime",
                nsmap,
            )
        )
        end_time = self._parse_datetime(
            self._text(
                record,
                "sit:validity/com:validityTimeSpecification/com:overallEndTime",
                nsmap,
            )
        )

        # --- Cause ---
        cause_type = self._text(record, "sit:cause/sit:causeType", nsmap)
        detailed_cause_type = self._text(
            record,
            "sit:cause/sit:detailedCauseType/sit:roadMaintenanceType",
            nsmap,
        )

        # --- Restriction ---
        management_type = self._text(
            record,
            "sit:roadOrCarriagewayOrLaneManagementType",
            nsmap,
        )
        vehicle_type = self._text(
            record,
            "sit:forVehiclesWithCharacteristicsOf/com:vehicleType",
            nsmap,
        )

        # --- Location reference ---
        loc_ref = record.find("sit:locationReference", nsmap)
        road_name: str | None = None
        road_destination: str | None = None
        direction: str | None = None
        carriageway: str | None = None
        lane_usage: str | None = None
        location_from: LocationPoint | None = None
        location_to: LocationPoint | None = None

        if loc_ref is not None:
            # Road info (shared across both location types)
            road_name = self._text(
                loc_ref,
                "loc:supplementaryPositionalDescription/loc:roadInformation/loc:roadName",
                nsmap,
            )
            road_destination = self._text(
                loc_ref,
                "loc:supplementaryPositionalDescription/loc:roadInformation/loc:roadDestination",
                nsmap,
            )
            carriageway = self._text(
                loc_ref,
                "loc:supplementaryPositionalDescription/loc:carriageway/loc:carriageway",
                nsmap,
            )
            lane_usage = self._text(
                loc_ref,
                "loc:supplementaryPositionalDescription/loc:carriageway/loc:lane/loc:laneUsage",
                nsmap,
            )

            # Branch on location type
            loc_type = loc_ref.get(f"{{{nsmap.get('xsi', '')}}}type", "")

            if "SingleRoadLinearLocation" in loc_type:
                location_from, location_to, direction = self._parse_linear_location(
                    loc_ref, nsmap
                )
            elif "PointLocation" in loc_type:
                location_from, direction = self._parse_point_location(loc_ref, nsmap)

        return TruckDashboardAlert(
            situation_id=situation_id,
            record_id=record_id,
            creation_time=creation_time,
            version_time=version_time,
            severity=severity,
            start_time=start_time,
            end_time=end_time,
            management_type=management_type,
            vehicle_type=vehicle_type,
            cause_type=cause_type,
            detailed_cause_type=detailed_cause_type,
            road_name=road_name,
            road_destination=road_destination,
            direction=direction,
            carriageway=carriageway,
            lane_usage=lane_usage,
            location_from=location_from,
            location_to=location_to,
        )

    def _parse_tpeg_point(
        self, point_el: etree.ElementBase, nsmap: dict[str, str]
    ) -> LocationPoint:
        """Extract a LocationPoint from a TpegNonJunctionPoint element.

        Args:
            point_el: The ``<loc:from>``, ``<loc:to>``, or ``<loc:point>``
                element.
            nsmap: Namespace map.

        Returns:
            Populated LocationPoint.
        """
        lat = self._text(point_el, "loc:pointCoordinates/loc:latitude", nsmap)
        lon = self._text(point_el, "loc:pointCoordinates/loc:longitude", nsmap)

        ext_path = "loc:_tpegNonJunctionPointExtension/loc:extendedTpegNonJunctionPoint"
        km = self._text(point_el, f"{ext_path}/lse:kilometerPoint", nsmap)
        community = self._text(point_el, f"{ext_path}/lse:autonomousCommunity", nsmap)
        province = self._text(point_el, f"{ext_path}/lse:province", nsmap)
        municipality = self._text(point_el, f"{ext_path}/lse:municipality", nsmap)

        return LocationPoint(
            latitude=self._float_or_none(lat),
            longitude=self._float_or_none(lon),
            km_point=self._float_or_none(km),
            community=community,
            province=province,
            municipality=municipality,
        )

    def _parse_linear_location(
        self, loc_ref: etree.ElementBase, nsmap: dict[str, str]
    ) -> tuple[LocationPoint | None, LocationPoint | None, str | None]:
        """Parse a ``SingleRoadLinearLocation`` into from/to points.

        Args:
            loc_ref: The ``<sit:locationReference>`` element.
            nsmap: Namespace map.

        Returns:
            Tuple of ``(location_from, location_to, direction)``.
        """
        linear = loc_ref.find("loc:tpegLinearLocation", nsmap)
        if linear is None:
            return None, None, None

        from_el = linear.find("loc:from", nsmap)
        to_el = linear.find("loc:to", nsmap)

        location_from = (
            self._parse_tpeg_point(from_el, nsmap) if from_el is not None else None
        )
        location_to = (
            self._parse_tpeg_point(to_el, nsmap) if to_el is not None else None
        )

        direction = self._text(
            linear,
            "loc:_tpegLinearLocationExtension/loc:extendedTpegLinearLocation/lse:tpegDirectionRoad",
            nsmap,
        )
        return location_from, location_to, direction

    def _parse_point_location(
        self, loc_ref: etree.ElementBase, nsmap: dict[str, str]
    ) -> tuple[LocationPoint | None, str | None]:
        """Parse a ``PointLocation`` into a single point.

        Args:
            loc_ref: The ``<sit:locationReference>`` element.
            nsmap: Namespace map.

        Returns:
            Tuple of ``(location_from, direction)``.
        """
        point_loc = loc_ref.find("loc:tpegPointLocation", nsmap)
        if point_loc is None:
            return None, None

        point_el = point_loc.find("loc:point", nsmap)
        location = (
            self._parse_tpeg_point(point_el, nsmap) if point_el is not None else None
        )

        direction = self._text(
            point_loc,
            "loc:_tpegSimplePointExtension/loc:extendedTpegSimplePoint/lse:tpegDirectionRoad",
            nsmap,
        )
        return location, direction

    @staticmethod
    def _is_non_truck_only(alert: TruckDashboardAlert) -> bool:
        """Check if an alert's vehicle type is exclusively non-truck.

        Args:
            alert: The alert to check.

        Returns:
            ``True`` if the alert is irrelevant to trucks.
        """
        if not alert.vehicle_type:
            return False
        return alert.vehicle_type.lower() in NON_TRUCK_VEHICLE_TYPES
