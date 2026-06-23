"""Pydantic data models for DATEX II traffic alert parsing.

Defines the ``TruckDashboardAlert`` model — one instance per
``situationRecord`` — and the supporting ``LocationPoint`` model used
for coordinate / admin-data pairs.
"""

from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class AlertConfidence(StrEnum):
    """Confidence classification assigned by the heuristic zombie filter.

    Attributes:
        VERIFIED_ACTIVE: Passes all heuristic rules. Show prominently.
        SUSPICIOUS: Approaching TTL limit. Render as faded/semi-transparent.
        ZOMBIE: Exceeds TTL entirely. Dropped from output payload.
    """

    VERIFIED_ACTIVE = "verified_active"
    SUSPICIOUS = "suspicious"
    ZOMBIE = "zombie"


class LocationPoint(BaseModel):
    """A single geographic reference point with administrative metadata.

    Attributes:
        latitude: WGS-84 latitude.
        longitude: WGS-84 longitude.
        km_point: Kilometer-point marker on the road, if available.
        reference_marker: Provider reference marker, such as a French PR marker
            or Dutch VILD ``LOC_NR``.
        offset_m: Offset in meters from the French PR marker or Alert-C
            reference, if available.
        alertc_location_id: Alert-C/TMC location identifier, if available.
        alertc_location_name: Human-readable Alert-C location name, if
            available.
        alertc_road_number: VILD road number for the Alert-C location.
        alertc_road_name: VILD road name for the Alert-C location.
        alertc_road_label: Combined VILD road number/name.
        alertc_first_name: VILD primary location name.
        alertc_second_name: VILD secondary location name.
        alertc_place_name: Combined VILD place/location name.
        alertc_area_name: Nearby VILD administrative area name.
        alertc_location_type: VILD location type code.
        alertc_location_description: Human-readable VILD location type.
        community: Spanish Autonomous Community (e.g. ``Andalucía``).
        province: Province name (e.g. ``Sevilla``).
        municipality: Municipality name.
    """

    model_config = ConfigDict(strict=False)

    latitude: float | None = None
    longitude: float | None = None
    km_point: float | None = None
    reference_marker: str | None = None
    offset_m: float | None = None
    alertc_location_id: str | None = None
    alertc_location_name: str | None = None
    alertc_road_number: str | None = None
    alertc_road_name: str | None = None
    alertc_road_label: str | None = None
    alertc_first_name: str | None = None
    alertc_second_name: str | None = None
    alertc_place_name: str | None = None
    alertc_area_name: str | None = None
    alertc_location_type: str | None = None
    alertc_location_description: str | None = None
    alertc_area_ref: int | None = None
    alertc_line_ref: int | None = None
    km_start_pos: float | None = None
    km_end_pos: float | None = None
    km_start_neg: float | None = None
    km_end_neg: float | None = None
    community: str | None = None
    province: str | None = None
    municipality: str | None = None


# Vehicle types that do NOT affect trucks.  Alerts restricted to only
# these types are excluded by default (truck_only mode).
NON_TRUCK_VEHICLE_TYPES: frozenset[str] = frozenset(
    {
        "bicycle",
        "moped",
        "motorcycle",
        "pedalCycle",
        "electricVehicle",
    }
)


class TruckDashboardAlert(BaseModel):
    """Flattened representation of a single DATEX II ``situationRecord``.

    One ``Situation`` can contain multiple records (e.g. several lane
    closures on the same road segment).  The parser yields **one**
    ``TruckDashboardAlert`` per record.

    Attributes:
        situation_id: Parent ``<sit:situation id="…">`` value.
        record_id: The ``<sit:situationRecord id="…">`` value.
        creation_time: When the record was first created.
        severity: Overall severity (highest / high / medium / low).
        start_time: When the restriction became active.
        end_time: When the restriction ends (``None`` = ongoing).
        management_type: Road/carriageway management type
            (e.g. ``roadClosed``, ``laneClosures``, ``narrowLanes``).
        vehicle_type: Which vehicles are affected
            (e.g. ``anyVehicle``, ``heavyGoodsVehicle``).
        road_name: Road designation (e.g. ``A-8``, ``AP-7``).
        road_destination: Directional destination text (e.g. ``IRUN``).
        direction: Direction of the restriction
            (``positive`` / ``negative`` / ``both``).
        cause_type: Broad cause category (e.g. ``roadMaintenance``).
        detailed_cause_type: Specific cause (e.g. ``roadworks``).
        carriageway: Carriageway info (e.g. ``unspecifiedCarriageway``).
        lane_usage: Lane restriction info (e.g. ``rightLane``).
        location_from: Start point of the affected segment.
        location_to: End point of the affected segment.
        public_comments: Human-readable DATEX public comments, when
            available.
        safety_related_message: French DATEX II v2 safety-related flag, when
            available.
    """

    model_config = ConfigDict(strict=False)

    # --- Metadata ---
    situation_id: str
    record_id: str
    creation_time: AwareDatetime | None = None
    version_time: AwareDatetime | None = None

    # --- Urgency ---
    severity: str | None = None

    # --- Timeframes ---
    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None

    # --- Restriction details ---
    management_type: str | None = None
    vehicle_type: str | None = None
    cause_type: str | None = None
    detailed_cause_type: str | None = None
    carriageway: str | None = None
    lane_usage: str | None = None

    # --- Location / routing ---
    road_name: str | None = None
    road_destination: str | None = None
    direction: str | None = None

    # --- Location / precise ---
    location_from: LocationPoint | None = None
    location_to: LocationPoint | None = None
    public_comments: list[str] = Field(default_factory=list)
    safety_related_message: bool | None = None
