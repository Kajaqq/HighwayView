from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from config import CONSTANTS

from .datex_filter import FilterConfig, HeuristicFilter, SEVERITY_RANK
from .datex_models import TruckDashboardAlert


class AlertParser(Protocol):
    @property
    def alerts(self) -> list[TruckDashboardAlert]: ...
    async def get_parsed_data(self) -> list[TruckDashboardAlert]: ...


def _is_road_closed(alert: TruckDashboardAlert) -> bool:
    return (alert.management_type or "").lower() == "roadclosed"


def _is_medium_or_higher(alert: TruckDashboardAlert) -> bool:
    default_rank = SEVERITY_RANK["medium"]
    rank = SEVERITY_RANK.get((alert.severity or "").lower(), default_rank)
    return rank >= SEVERITY_RANK["medium"]


def _is_overlay_relevant(alert: TruckDashboardAlert) -> bool:
    return _is_medium_or_higher(alert) or _is_road_closed(alert)


def _serialize_location(alert: TruckDashboardAlert, field_name: str) -> dict[str, Any]:
    location = getattr(alert, field_name)
    return {
        "latitude": location.latitude if location else None,
        "longitude": location.longitude if location else None,
        "km_point": location.km_point if location else None,
        "reference_marker": location.reference_marker if location else None,
        "offset_m": location.offset_m if location else None,
        "alertc_location_id": location.alertc_location_id if location else None,
        "alertc_location_name": location.alertc_location_name if location else None,
        "community": location.community if location else None,
        "province": location.province if location else None,
        "municipality": location.municipality if location else None,
    }


def _serialize_alert(alert: TruckDashboardAlert, confidence: str) -> dict[str, Any]:
    return {
        "situation_id": alert.situation_id,
        "record_id": alert.record_id,
        "confidence": confidence,
        "severity": alert.severity,
        "management_type": alert.management_type,
        "cause_type": alert.cause_type,
        "detailed_cause_type": alert.detailed_cause_type,
        "road_name": alert.road_name,
        "road_destination": alert.road_destination,
        "direction": alert.direction,
        "creation_time": alert.creation_time.isoformat()
        if alert.creation_time
        else None,
        "version_time": alert.version_time.isoformat() if alert.version_time else None,
        "start_time": alert.start_time.isoformat() if alert.start_time else None,
        "end_time": alert.end_time.isoformat() if alert.end_time else None,
        "location_from": _serialize_location(alert, "location_from"),
        "location_to": _serialize_location(alert, "location_to"),
        "public_comments": alert.public_comments,
        "safety_related_message": alert.safety_related_message,
    }


async def build_overlay_payload(
    parser: AlertParser,
    roads: list[str] | None = None,
    max_items: int = 50,
    filter_config: FilterConfig | None = None,
    skip_filter: bool = False,
) -> dict[str, Any]:
    await parser.get_parsed_data()

    alerts = parser.alerts
    if roads:
        road_set = set(roads)
        alerts = [a for a in alerts if a.road_name in road_set]

    if skip_filter:
        merged = [_serialize_alert(a, "unfiltered") for a in alerts]
    else:
        alerts = [alert for alert in alerts if _is_overlay_relevant(alert)]
        heuristic = HeuristicFilter(config=filter_config)
        result = heuristic.filter(alerts)
        active = [_serialize_alert(a, "verified_active") for a in result.active]
        suspicious = [_serialize_alert(a, "suspicious") for a in result.suspicious]
        merged = active + suspicious

    merged.sort(
        key=lambda item: (
            SEVERITY_RANK.get((item.get("severity") or "").lower(), 0),
            item.get("version_time")
            or item.get("creation_time")
            or item.get("start_time")
            or "",
        ),
        reverse=True,
    )

    if max_items > 0:
        merged = merged[:max_items]

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "total": len(merged),
        "roads_filter": roads or [],
        "severity_rule": "none (debug)"
        if skip_filter
        else "medium_or_higher_plus_road_closed",
        "alerts": merged,
    }


def write_overlay_payload(payload: dict[str, Any], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


async def export_overlay_data(
    parser: AlertParser,
    output_file: Path | None = None,
    roads: list[str] | None = None,
    max_items: int = 50,
    filter_config: FilterConfig | None = None,
    skip_filter: bool = False,
) -> Path:
    target = output_file or (CONSTANTS.COMMON.DATA_DIR / "overlay_data.json")
    payload = await build_overlay_payload(
        roads=roads,
        max_items=max_items,
        filter_config=filter_config,
        parser=parser,
        skip_filter=skip_filter,
    )
    write_overlay_payload(payload, target)
    return target


async def run_overlay_export_loop(
    parser: AlertParser,
    country_code: str,
    interval_seconds: int = 300,
    output_file: Path | None = None,
    roads: list[str] | None = None,
    max_items: int = 50,
    filter_config: FilterConfig | None = None,
    skip_filter: bool = False,
) -> None:
    while True:
        target = await export_overlay_data(
            output_file=output_file,
            roads=roads,
            max_items=max_items,
            filter_config=filter_config,
            parser=parser,
            skip_filter=skip_filter,
        )
        print(f"[{country_code}] Overlay data updated: {target}")
        await asyncio.sleep(interval_seconds)
