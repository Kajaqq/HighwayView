from __future__ import annotations

import argparse
import asyncio
import json
import socket
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import winloop
from config import CONSTANTS
from lxml import html
from tqdm import tqdm

DEFAULT_INPUT = Path("tools/tmc/tmc_data.json")
DEFAULT_OUTPUT = Path("tools/tmc/tmc_location_summaries.json")
DEFAULT_URL_TEMPLATE = (
    "https://mhohmann.dev.openstreetmap.org/tmc/tmcview.php?cid=6&tabcd=1&lcd={id}"
)
DEFAULT_RATE_LIMIT = 2
DEFAULT_REQUEST_DELAY = 0.5
DEFAULT_RETRIES = 4
DEFAULT_RETRY_DELAY = 30
ERROR_SAMPLE_COUNT = 10
RETRY_STATUSES = {429, 500, 502, 503, 504}
REMOTE_EXCLUDED_CATEGORY_CODES = ("A*", "P2.1", "P3.3", "P4.0")

type SummaryValue = str | list[str]
type Fields = dict[str, SummaryValue]
type OutputGroup = dict[str, dict[str, Fields]]
type OutputPayload = dict[str, OutputGroup]
type FetchResult = tuple[LocationInput, Fields, str | None]


@dataclass(frozen=True)
class LocationInput:
    id: str
    source_name: str
    category_code: str
    category: str


def clean_text(value: str) -> str:
    return " ".join(value.split())


def short_category(category):
    return category.rsplit(" - ", maxsplit=1)[-1]


def group_key(category_code):
    match category_code[:1]:
        case "A":
            return "locations"
        case "L":
            return "roads"
        case "P":
            return "points"
        case _:
            return "Unknown"


def category_key(location: LocationInput) -> str:
    return f"{location.category_code} - {short_category(location.category)}"


def add_summary(
    payload: OutputPayload,
    location: LocationInput,
    fields: Fields,
) -> None:
    key = group_key(location.category_code)
    payload.setdefault(key, {}).setdefault(category_key(location), {})[location.id] = (
        fields
    )


def load_locations(input_path: Path) -> list[LocationInput]:
    raw_entries = json.loads(input_path.read_text(encoding="utf-8"))
    locations: list[LocationInput] = []

    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue

        category_code = ""
        category = ""
        for key, value in entry.items():
            if not key.isdigit():
                category_code = key
                category = str(value)
                break

        for key, value in entry.items():
            if key.isdigit():
                locations.append(
                    LocationInput(
                        id=key,
                        source_name=str(value),
                        category_code=category_code,
                        category=category,
                    )
                )

    return locations


def load_existing_payload(output_path: Path) -> OutputPayload:
    payload: OutputPayload = {
        "locations": {},
        "roads": {},
        "points": {},
    }
    if not output_path.exists():
        return payload

    raw_payload = json.loads(output_path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        return payload

    for key in payload:
        categories = raw_payload.get(key, {})
        if isinstance(categories, dict):
            payload[key] = {
                category: records
                for category, records in categories.items()
                if isinstance(category, str) and isinstance(records, dict)
            }
        elif isinstance(categories, list):
            for record in categories:
                if not isinstance(record, dict):
                    continue
                record_id = record.get("id")
                category_code = record.get("category_code")
                category = record.get("category")
                fields = record.get("fields", {})
                if not all(
                    isinstance(value, str)
                    for value in (record_id, category_code, category)
                ) or not isinstance(fields, dict):
                    continue
                payload[key].setdefault(f"{category_code} - {category}", {})[
                    record_id
                ] = fields

    return payload


def existing_summary_ids(payload: OutputPayload) -> set[str]:
    ids: set[str] = set()
    for categories in payload.values():
        for records in categories.values():
            ids.update(record_id for record_id in records if isinstance(record_id, str))
    return ids


def add_summary_value(
    summary: dict[str, SummaryValue],
    key: str,
    value: str,
) -> None:
    existing = summary.get(key)
    if existing is None:
        summary[key] = value
    elif isinstance(existing, list):
        existing.append(value)
    else:
        summary[key] = [existing, value]


def parse_summary_fields(content: bytes) -> Fields:
    try:
        document = html.fromstring(content)
    except html.etree.ParserError:
        return {}

    headings = document.xpath("//h3[normalize-space(.) = 'Summary and tools']")
    if not headings:
        return {}

    lists = headings[0].xpath("following-sibling::ul[1]")
    if not lists:
        return {}

    fields: Fields = {}
    for list_item in lists[0].xpath(".//li"):
        item = clean_text(list_item.text_content())
        if not item:
            continue
        key, separator, value = item.partition(":")
        if separator:
            add_summary_value(fields, key.strip(), value.strip())

    return fields


async def fetch_location(
    session: aiohttp.ClientSession,
    location: LocationInput,
    url_template: str,
    retries: int,
    retry_delay: float,
) -> tuple[Fields, str | None]:
    url = url_template.format(id=location.id)
    for attempt in range(retries + 1):
        try:
            async with session.get(url, allow_redirects=True) as response:
                content = await response.read()
                if response.status in RETRY_STATUSES and attempt < retries:
                    wait_seconds = retry_delay * (attempt + 1)
                    tqdm.write(
                        f"{location.id}: HTTP {response.status}; "
                        f"retry {attempt + 1}/{retries} in {wait_seconds:g}s"
                    )
                    await asyncio.sleep(wait_seconds)
                    continue

                fields = parse_summary_fields(content)
                if response.status >= 400:
                    return (
                        {},
                        f"{location.id}: HTTP {response.status}",
                    )
                if not fields:
                    return (
                        {},
                        f"{location.id}: Summary and tools section not found",
                    )

                return fields, None
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            if attempt < retries:
                wait_seconds = retry_delay * (attempt + 1)
                tqdm.write(
                    f"{location.id}: {type(exc).__name__}: {exc}; "
                    f"retry {attempt + 1}/{retries} in {wait_seconds:g}s"
                )
                await asyncio.sleep(wait_seconds)
                continue
            return {}, f"{location.id}: {exc}"

    return {}, f"{location.id}: Retry attempts exhausted"


async def fetch_worker(
    queue: asyncio.Queue[tuple[int, LocationInput]],
    session: aiohttp.ClientSession,
    url_template: str,
    results: list[FetchResult | None],
    progress: tqdm,
    request_delay: float,
    retries: int,
    retry_delay: float,
) -> None:
    while True:
        try:
            index, location = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            fields, error = await fetch_location(
                session,
                location,
                url_template,
                retries,
                retry_delay,
            )
            results[index] = (location, fields, error)
            progress.update()
            if error is not None:
                tqdm.write(f"{error}; giving up")
            if request_delay > 0:
                await asyncio.sleep(request_delay)
        finally:
            queue.task_done()


def _get_http_settings(rate_limit: int, timeout: int):

    headers: dict[str, str] = CONSTANTS.COMMON.DEFAULT_HEADERS.copy()
    timeout = aiohttp.ClientTimeout(total=timeout)

    # This shouldn't be required, but for some reason it is
    resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])
    connector = aiohttp.TCPConnector(
        resolver=resolver,
        limit=rate_limit,
        ttl_dns_cache=300,
        family=socket.AF_INET,
    )
    return headers, timeout, connector


async def fetch_locations(
    locations: list[LocationInput],
    url_template: str,
    rate_limit: int,
    request_delay: float,
    retries: int,
    retry_delay: float,
) -> list[FetchResult]:
    if not locations:
        return []

    queue: asyncio.Queue[tuple[int, LocationInput]] = asyncio.Queue()
    for index, location in enumerate(locations):
        queue.put_nowait((index, location))

    headers, timeout, connector = _get_http_settings(rate_limit, 30)

    results: list[FetchResult | None] = [None] * len(locations)
    worker_count = min(rate_limit, len(locations))

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=headers
    ) as session:
        with tqdm(
            total=len(locations),
            desc="Fetching roads/points",
            unit="page",
        ) as progress:
            tqdm.write(
                f"Fetch policy: workers={worker_count}, request_delay={request_delay:g}s, "
                f"retries={retries}, retry_delay={retry_delay:g}s"
            )
            await asyncio.gather(
                *(
                    fetch_worker(
                        queue,
                        session,
                        url_template,
                        results,
                        progress,
                        request_delay,
                        retries,
                        retry_delay,
                    )
                    for _ in range(worker_count)
                )
            )

    return [result for result in results if result is not None]


async def download_summaries(
    input_path: Path,
    output_path: Path,
    rate_limit: int,
    request_delay: float,
    retries: int,
    retry_delay: float,
    limit: int | None,
) -> None:
    url_template = DEFAULT_URL_TEMPLATE
    locations = await asyncio.to_thread(load_locations, input_path)
    if limit is not None:
        locations = locations[:limit]

    payload = await asyncio.to_thread(load_existing_payload, output_path)
    existing_ids = existing_summary_ids(payload)

    fetchable_locations: list[LocationInput] = []
    local_copy_count = 0
    skipped_existing_count = 0
    for location in locations:
        if location.id in existing_ids:
            skipped_existing_count += 1
            continue

        key = group_key(location.category_code)
        if key is None:
            continue
        remote_excluded = any(
            location.category_code.startswith(code[:-1])
            if code.endswith("*")
            else location.category_code == code
            for code in REMOTE_EXCLUDED_CATEGORY_CODES
        )
        if remote_excluded:
            add_summary(payload, location, {"Name": location.source_name})
            local_copy_count += 1
        elif " " not in clean_text(location.source_name):
            add_summary(payload, location, {"Name": location.source_name})
            local_copy_count += 1
        else:
            fetchable_locations.append(location)

    results = await fetch_locations(
        fetchable_locations,
        url_template,
        rate_limit,
        request_delay,
        retries,
        retry_delay,
    )

    errors: list[str] = []
    for location, fields, error in results:
        key = group_key(location.category_code)
        if key is not None:
            add_summary(payload, location, fields)
        if error is not None:
            errors.append(error)

    await asyncio.to_thread(output_path.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(
        output_path.write_text,
        json.dumps(payload, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    resolved_output = await asyncio.to_thread(output_path.resolve)

    print(f"Loaded {len(locations)} location IDs from {input_path}.")
    print(f"Skipped {skipped_existing_count} existing summaries from {output_path}.")
    print(f"Fetched {len(results) - len(errors)}/{len(results)} road/point summaries.")
    print(f"Copied {local_copy_count} summaries from local names.")
    if errors:
        print(f"Encountered {len(errors)} fetch or parse errors.")
        for error in errors[:ERROR_SAMPLE_COUNT]:
            print(f"  - {error}")
        if len(errors) > ERROR_SAMPLE_COUNT:
            print(f"  ... {len(errors) - ERROR_SAMPLE_COUNT} more errors")
    print(f"Wrote {resolved_output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download TMC location pages and extract their Summary and tools lists.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="TMC location JSON containing numeric IDs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="JSON file to write parsed location summaries into.",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=DEFAULT_RATE_LIMIT,
        help="Maximum number of location requests to run at once.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help="Seconds to wait after each road/point request.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Retry count for HTTP 429 and transient server errors.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY,
        help="Base seconds to wait before retrying; later retries use a multiple.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Fetch only the first N IDs. Useful for quick checks.",
    )

    args = parser.parse_args()
    if args.rate_limit < 1:
        parser.error("--rate-limit must be at least 1")
    if args.request_delay < 0:
        parser.error("--request-delay must be non-negative")
    if args.retries < 0:
        parser.error("--retries must be non-negative")
    if args.retry_delay < 0:
        parser.error("--retry-delay must be non-negative")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    return args


if __name__ == "__main__":
    args = parse_args()
    winloop.run(
        download_summaries(
            input_path=args.input,
            output_path=args.output,
            rate_limit=args.rate_limit,
            request_delay=args.request_delay,
            retries=args.retries,
            retry_delay=args.retry_delay,
            limit=args.limit,
        )
    )
