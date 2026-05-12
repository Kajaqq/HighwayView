"""Heuristic zombie filter for DATEX II traffic alerts.

Middleware between the XML parser and the dashboard delivery layer.
Classifies each :class:`TruckDashboardAlert` as *verified active*,
*suspicious*, or *zombie* based on configurable cause-based TTL rules,
severity overrides, and fail-safe mechanisms.

Example::

    from DatexParser.datex_filter import HeuristicFilter

    heuristic = HeuristicFilter()
    result = heuristic.filter(raw_alerts)
    # result.active   → show prominently
    # result.suspicious → show faded / semi-transparent
    # result.dropped_count → zombies removed
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .datex_models import AlertConfidence, TruckDashboardAlert

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cause-type classification buckets
# ---------------------------------------------------------------------------

#: Transient events that should clear within days.
TRANSIENT_CAUSES: frozenset[str] = frozenset(
    {
        "accident",
        "vehicleobstruction",
        "animalpresence",
        "poorweatherconditions",
        "poorenvironmentconditions",
        "abnormaltraffic",
    }
)

#: Major infrastructure issues that persist for years.
INFRASTRUCTURE_CAUSES: frozenset[str] = frozenset(
    {
        "infrastructuredamageobstruction",
    }
)

#: Management types considered non-blocking for the severity override.
NON_BLOCKING_MANAGEMENT: frozenset[str] = frozenset(
    {
        "narrowlanes",
        "singlealternatelinetraffic",
        "newroadworkslayout",
        "donotusespecifiedlanesorcarriageways",
    }
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FilterConfig:
    """Tunable thresholds for the heuristic zombie filter.

    All TTL values are in days.

    Attributes:
        transient_ttl_days: Max lifespan for transient events
            (accidents, weather, animal presence).
        roadworks_ttl_days: Max lifespan for standard road maintenance.
        infrastructure_ttl_days: Max lifespan for major infrastructure
            damage (collapsed bridges, landslides).
        low_severity_ttl_days: Override TTL for low/null severity +
            non-blocking management types.
        highest_road_closed_bonus: Bonus days added to TTL when
            severity is "highest" AND management is "roadClosed".
        suspicious_threshold: Fraction of TTL at which an alert
            transitions from verified_active to suspicious (0.0–1.0).
    """

    transient_ttl_days: int = 3
    roadworks_ttl_days: int = 180
    infrastructure_ttl_days: int = 1095  # ~3 years
    low_severity_ttl_days: int = 30
    highest_road_closed_bonus: int = 365
    suspicious_threshold: float = 0.75


# ---------------------------------------------------------------------------
# Filter result
# ---------------------------------------------------------------------------


@dataclass
class FilterResult:
    """Container for heuristic filter output.

    Attributes:
        active: Alerts classified as ``VERIFIED_ACTIVE``.
        suspicious: Alerts classified as ``SUSPICIOUS``.
        dropped_count: Number of zombie records removed.
    """

    active: list[TruckDashboardAlert] = field(default_factory=list)
    suspicious: list[TruckDashboardAlert] = field(default_factory=list)
    dropped_count: int = 0

    def all_active(self) -> list[TruckDashboardAlert]:
        """Return both active and suspicious alerts combined.

        Returns:
            Merged list of non-zombie alerts, sorted newest-first.
        """
        return sorted(self.active + self.suspicious, key=_sort_key, reverse=True)

    def sort_by_recency(self) -> None:
        """Sort ``active`` and ``suspicious`` lists in-place, newest first.

        Uses ``version_time`` as the primary key, falling back to
        ``start_time``.  Alerts with no timestamp sort last.
        """
        self.active.sort(key=_sort_key, reverse=True)
        self.suspicious.sort(key=_sort_key, reverse=True)


def _sort_key(alert: TruckDashboardAlert) -> datetime:
    """Return the best available recency timestamp for sorting.

    Args:
        alert: The alert to extract a timestamp from.

    Returns:
        A timezone-aware datetime (``datetime.min`` with UTC if none available).
    """
    return alert.version_time or alert.start_time or datetime.min.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# The engine
# ---------------------------------------------------------------------------


#: Severity levels ranked from lowest to highest.
SEVERITY_RANK: dict[str, int] = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "highest": 3,
}


class HeuristicFilter:
    """Cause-based TTL zombie filter for DATEX II alerts.

    Implements a configurable rules engine that classifies each alert
    into one of three confidence buckets based on its age, cause type,
    severity, and management type.

    Args:
        config: Tunable thresholds.  Uses sensible defaults if omitted.
    """

    def __init__(self, config: FilterConfig | None = None) -> None:
        self.config = config or FilterConfig()

    def filter(
        self,
        alerts: list[TruckDashboardAlert],
        min_severity: str | None = None,
        now: datetime | None = None,
    ) -> FilterResult:
        """Run the heuristic engine over a list of parsed alerts.

        Args:
            alerts: Raw alerts from the DATEX II parser.
            min_severity: Optional minimum severity threshold.
                Only alerts at or above this level are processed
                (one of ``"low"``, ``"medium"``, ``"high"``, ``"highest"``).
                Alerts with ``None`` severity are kept regardless.
            now: Reference time for age calculations.  Defaults to
                ``datetime.now(UTC)`` when ``None``.

        Returns:
            A :class:`FilterResult` with categorized alerts.
        """
        now = now or datetime.now(UTC)
        result = FilterResult()

        if min_severity:
            alerts = self.filter_by_severity(alerts, min_severity)

        for alert in alerts:
            confidence = self._classify(alert, now)

            if confidence == AlertConfidence.VERIFIED_ACTIVE:
                result.active.append(alert)
            elif confidence == AlertConfidence.SUSPICIOUS:
                result.suspicious.append(alert)
            else:
                result.dropped_count += 1
                logger.info(
                    "DROPPED %s (record %s): %s",
                    alert.situation_id,
                    alert.record_id,
                    self._drop_reason(alert, now),
                )

        logger.info(
            "Heuristic filter: %d active, %d suspicious, %d zombie (dropped)",
            len(result.active),
            len(result.suspicious),
            result.dropped_count,
        )
        result.sort_by_recency()
        return result

    @staticmethod
    def filter_by_severity(
        alerts: list[TruckDashboardAlert],
        min_severity: str,
    ) -> list[TruckDashboardAlert]:
        """Filter alerts by minimum severity level.

        Alerts whose severity is ``None`` are treated as ``"medium"``.

        Args:
            alerts: List of alerts to filter.
            min_severity: Minimum severity threshold (``"low"``,
                ``"medium"``, ``"high"``, or ``"highest"``).

        Returns:
            Alerts at or above the requested severity.
        """
        min_rank = SEVERITY_RANK.get(min_severity.lower(), 0)
        default_rank = SEVERITY_RANK["medium"]
        return [
            a
            for a in alerts
            if SEVERITY_RANK.get((a.severity or "").lower(), default_rank) >= min_rank
        ]

    # ------------------------------------------------------------------
    # Classification pipeline
    # ------------------------------------------------------------------

    def _classify(self, alert: TruckDashboardAlert, now: datetime) -> AlertConfidence:
        """Classify a single alert through the rules pipeline.

        Args:
            alert: The alert to classify.
            now: Current UTC time.

        Returns:
            The confidence classification.
        """
        # Step 1: Future-event bypass
        if alert.start_time and alert.start_time > now:
            return AlertConfidence.VERIFIED_ACTIVE

        # Step 2: Determine base TTL from cause type
        ttl_days = self._get_cause_ttl(alert)

        # Step 3: Severity & impact override
        ttl_days = self._apply_severity_override(alert, ttl_days)

        # Step 4: Highest-severity fail-safe
        ttl_days = self._apply_fail_safe(alert, ttl_days)

        # Step 5: Calculate age
        age_days = self._age_in_days(alert, now)
        if age_days is None:
            # Cannot determine age — keep it as active to be safe
            return AlertConfidence.VERIFIED_ACTIVE

        # Step 6: Classification
        suspicious_limit = ttl_days * self.config.suspicious_threshold
        if age_days < suspicious_limit:
            return AlertConfidence.VERIFIED_ACTIVE
        if age_days < ttl_days:
            return AlertConfidence.SUSPICIOUS
        return AlertConfidence.ZOMBIE

    def _get_cause_ttl(self, alert: TruckDashboardAlert) -> int:
        """Look up the base TTL from the cause-type classification.

        Args:
            alert: The alert to evaluate.

        Returns:
            TTL in days.
        """
        cause = (alert.cause_type or "").lower()

        if cause in TRANSIENT_CAUSES:
            return self.config.transient_ttl_days

        if cause in INFRASTRUCTURE_CAUSES:
            return self.config.infrastructure_ttl_days

        # Check if obstruction but NOT infrastructure (transient obstruction)
        if cause == "obstruction":
            return self.config.transient_ttl_days

        # Default: roadMaintenance and everything else
        return self.config.roadworks_ttl_days

    def _apply_severity_override(
        self, alert: TruckDashboardAlert, ttl_days: int
    ) -> int:
        """Reduce TTL for low-severity, non-blocking alerts.

        Args:
            alert: The alert to evaluate.
            ttl_days: Current TTL.

        Returns:
            Possibly reduced TTL.
        """
        severity = (alert.severity or "").lower()
        management = (alert.management_type or "").lower()

        if severity in ("low", "") and management in NON_BLOCKING_MANAGEMENT:
            return min(ttl_days, self.config.low_severity_ttl_days)

        return ttl_days

    def _apply_fail_safe(self, alert: TruckDashboardAlert, ttl_days: int) -> int:
        """Apply the 'highest severity + roadClosed' fail-safe bonus.

        A phantom road closure is annoying, but driving a 40-ton truck
        into a real, unmapped road closure is a disaster.

        Args:
            alert: The alert to evaluate.
            ttl_days: Current TTL.

        Returns:
            TTL with bonus days added if the fail-safe triggers.
        """
        severity = (alert.severity or "").lower()
        management = (alert.management_type or "").lower()

        if severity == "highest" and management == "roadclosed":
            return ttl_days + self.config.highest_road_closed_bonus

        return ttl_days

    @staticmethod
    def _age_in_days(alert: TruckDashboardAlert, now: datetime) -> float | None:
        """Calculate the alert's age in days from version_time or start_time.

        All comparisons are UTC-normalized.

        Args:
            alert: The alert to evaluate.
            now: Current UTC time.

        Returns:
            Age in fractional days, or ``None`` if no timestamp is available.
        """
        reference = alert.version_time or alert.start_time
        if reference is None:
            return None
        delta = now - reference
        return delta.total_seconds() / 86400

    @staticmethod
    def _drop_reason(alert: TruckDashboardAlert, now: datetime) -> str:
        """Build a human-readable reason string for a zombie drop.

        Args:
            alert: The dropped alert.
            now: Current UTC time.

        Returns:
            Descriptive reason string for telemetry logs.
        """
        reference = alert.version_time or alert.start_time
        if reference is None:
            return "No timestamp available"
        age = (now - reference).days
        cause = alert.cause_type or "unknown"
        road = alert.road_name or "unknown road"
        return f"Exceeded {cause} TTL (age: {age}d, road: {road})"
