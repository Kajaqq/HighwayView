from argparse import ArgumentParser, Namespace
from asyncio import gather
from pathlib import Path

import winloop
from config import CONSTANTS
from DatexParser.cciss_parser import CcissParser
from DatexParser.datex_filter import FilterConfig
from DatexParser.datex_parser import DatexParser
from DatexParser.overlay_export import export_overlay_data, run_overlay_export_loop
from Downloaders.base_downloader import GenericDownloader

COUNTRY_CONFIGS: dict[str, dict] = {
    "ES": {
        "roads": CONSTANTS.SPAIN.DATEX_ROADS,
        "output_dir": CONSTANTS.SPAIN.DATEX_OVERLAY_DIR,
        "filter_config": FilterConfig(
            transient_ttl_days=2,
            roadworks_ttl_days=1800,
            infrastructure_ttl_days=1095,
            low_severity_ttl_days=2,
            highest_road_closed_bonus=365,
            suspicious_threshold=0.75,
        ),
        "parser_kwargs": {"datex_url": CONSTANTS.SPAIN.DATEX_URL},
    },
    "FR": {
        "roads": CONSTANTS.FRANCE.DATEX_ROADS,
        "output_dir": CONSTANTS.FRANCE.DATEX_OVERLAY_DIR,
        "filter_config": FilterConfig(
            transient_ttl_days=2,
            roadworks_ttl_days=1800,
            infrastructure_ttl_days=1095,
            low_severity_ttl_days=2,
            highest_road_closed_bonus=365,
            suspicious_threshold=0.75,
        ),
        "parser_kwargs": {"datex_url": CONSTANTS.FRANCE.DATEX_URL},
    },
    "IT": {
        "roads": CONSTANTS.ITALY.DATEX_ROADS,
        "output_dir": CONSTANTS.ITALY.DATEX_OVERLAY_DIR,
        "filter_config": FilterConfig(
            transient_ttl_days=2,
            roadworks_ttl_days=3650,
            infrastructure_ttl_days=1095,
            low_severity_ttl_days=7,
            highest_road_closed_bonus=365,
            suspicious_threshold=0.75,
        ),
        "parser_kwargs": {},
    },
}


def parse_args() -> Namespace:

    parser = ArgumentParser(
        description="Export DATEX traffic alerts to overlay_data.json for OBS Browser Source."
    )
    parser.add_argument(
        "--country",
        choices=["ES", "FR", "IT", "all"],
        default="all",
        help="Which country to process (default: all).",
    )
    parser.add_argument(
        "--roads",
        default=None,
        help="Comma-separated road whitelist (overrides per-country defaults). Empty string disables road filtering.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Refresh interval in seconds when running in loop mode.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=1000,
        help="Maximum number of alerts to keep in the output file.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit. Default behavior runs continuously.",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Disable heuristic filtering and severity gating (debug mode).",
    )
    return parser.parse_args()


def _parse_roads(raw_roads: str) -> list[str]:
    return [road.strip() for road in raw_roads.split(",") if road.strip()]


def _build_parser(country: str) -> CcissParser | DatexParser:
    if country == "IT":
        return CcissParser(downloader=GenericDownloader())
    kwargs = COUNTRY_CONFIGS[country]["parser_kwargs"]
    return DatexParser(downloader=GenericDownloader(), **kwargs)


def _get_output_file(country: str) -> Path:
    return (
        CONSTANTS.COMMON.DATA_DIR
        / COUNTRY_CONFIGS[country]["output_dir"]
        / "overlay_data.json"
    )


async def _run_country_once(
    country: str,
    roads: list[str] | None,
    max_items: int,
    skip_filter: bool,
) -> None:
    cfg = COUNTRY_CONFIGS[country]
    effective_roads = roads if roads is not None else cfg["roads"]
    target = await export_overlay_data(
        output_file=_get_output_file(country),
        roads=effective_roads or None,
        max_items=max_items,
        filter_config=cfg["filter_config"],
        parser=_build_parser(country),
        skip_filter=skip_filter,
    )
    print(f"[{country}] Overlay data written to: {target}")


async def _run_country_loop(
    country: str,
    roads: list[str] | None,
    max_items: int,
    interval_seconds: int,
    skip_filter: bool,
) -> None:
    cfg = COUNTRY_CONFIGS[country]
    effective_roads = roads if roads is not None else cfg["roads"]
    await run_overlay_export_loop(
        interval_seconds=interval_seconds,
        output_file=_get_output_file(country),
        roads=effective_roads or None,
        max_items=max_items,
        filter_config=cfg["filter_config"],
        parser=_build_parser(country),
        skip_filter=skip_filter,
        country_code=country,
    )


async def main() -> None:
    args = parse_args()
    roads = (_parse_roads(args.roads) or None) if args.roads is not None else None
    countries = list(COUNTRY_CONFIGS) if args.country == "all" else [args.country]

    if args.once:
        await gather(
            *(
                _run_country_once(c, roads, args.max_items, args.no_filter)
                for c in countries
            )
        )
        return

    await gather(
        *(
            _run_country_loop(
                c, roads, args.max_items, args.interval_seconds, args.no_filter
            )
            for c in countries
        )
    )


if __name__ == "__main__":
    winloop.run(main())
