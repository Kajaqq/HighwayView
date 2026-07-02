from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

import aiohttp
import winloop
from lxml import html

DEFAULT_URL = "https://mhohmann.dev.openstreetmap.org/tmc/tmcview.php?cid=6&tabcd=1"
DEFAULT_OUTPUT = Path("data/tmc_data.json")
DEFAULT_RATE_LIMIT = 8
HTTP_SCHEMES = {"http", "https"}
SKIPPED_SCHEMES = {"data", "javascript", "mailto", "tel"}


def strip_fragment(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def same_origin(url: str, base_url: str) -> bool:
    url_parts = urlsplit(url)
    base_parts = urlsplit(base_url)
    return (url_parts.scheme, url_parts.netloc) == (
        base_parts.scheme,
        base_parts.netloc,
    )


def extract_child_links(
    content: bytes, base_url: str, include_external: bool
) -> list[str]:
    document = html.fromstring(content, base_url=base_url)
    base_without_fragment = strip_fragment(base_url)
    child_links: list[str] = []
    seen: set[str] = set()

    for href in document.xpath("//a[@href]/@href"):
        if urlsplit(href).scheme in SKIPPED_SCHEMES:
            continue

        child_url = strip_fragment(urljoin(base_url, href))
        child_parts = urlsplit(child_url)
        if child_parts.scheme not in HTTP_SCHEMES:
            continue
        if child_url == base_without_fragment:
            continue
        if not include_external and not same_origin(child_url, base_url):
            continue
        if child_url in seen:
            continue

        seen.add(child_url)
        child_links.append(child_url)

    return child_links


async def fetch(session: aiohttp.ClientSession, url: str) -> tuple[str, bytes]:
    async with session.get(url, allow_redirects=True) as response:
        content = await response.read()
        response.raise_for_status()
        return str(response.url), content


def parse_root_html(content: bytes, base_url: str) -> list[dict[str, str]]:
    document = html.fromstring(content, base_url=base_url)
    entries: list[dict[str, str]] = []

    for heading in document.xpath("//h2"):
        category = heading.text_content().strip()
        table = heading.xpath("following-sibling::table[1]")
        if not table:
            continue

        for row in table[0].xpath(".//tr[td]"):
            cells = row.xpath("./td")
            if len(cells) < 2:
                continue

            type_link = cells[0].xpath(".//a[1]")
            if not type_link:
                continue

            type_code = type_link[0].text_content().strip()
            description = cells[1].text_content().strip()
            entries.append({type_code: f"{category} - {description}"})

    return entries


def parse_child_html(content: bytes, base_url: str) -> tuple[str, dict[str, str]]:
    document = html.fromstring(content, base_url=base_url)
    locations: dict[str, str] = {}
    type_code = ""

    for row in document.xpath("//table[contains(@class, 'tmclist')]//tr[td]"):
        cells = row.xpath("./td")
        if len(cells) < 3:
            continue

        location_link = cells[0].xpath(".//a[1]")
        if location_link:
            query = parse_qs(urlsplit(location_link[0].get("href", "")).query)
            location_code = query.get("lcd", [""])[0]
        else:
            location_code = cells[0].text_content().strip().rsplit(":", maxsplit=1)[-1]

        if not location_code:
            continue

        type_code = cells[1].text_content().strip()
        locations[location_code] = cells[2].text_content().strip()

    return type_code, dict(sorted(locations.items(), key=lambda item: int(item[0])))


async def fetch_child_locations(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
) -> tuple[str, dict[str, str]]:
    try:
        async with semaphore:
            final_url, content = await fetch(session, url)
            return parse_child_html(content, final_url)
    except (aiohttp.ClientError, TimeoutError) as exc:
        print(f"Skipping {url}: {exc}")
        return "", {}


def merge_child_locations(
    entries: list[dict[str, str]], child_locations: list[tuple[str, dict[str, str]]]
) -> int:
    entries_by_type = {next(iter(entry)): entry for entry in entries}
    merged_count = 0

    for type_code, locations in child_locations:
        if not type_code or not locations:
            continue

        entry = entries_by_type.get(type_code)
        if entry is None:
            continue

        entry.update(locations)
        merged_count += 1

    return merged_count


async def build_tmc_data(
    url: str,
    rate_limit: int,
    include_external: bool,
    limit: int | None,
) -> tuple[list[dict[str, str]], int, int]:
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(
        limit=rate_limit, resolver=aiohttp.ThreadedResolver()
    )
    headers = {"User-Agent": "HighwayView TMC data downloader"}

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=headers
    ) as session:
        final_url, root_content = await fetch(session, url)
        entries = parse_root_html(root_content, final_url)
        child_links = extract_child_links(root_content, final_url, include_external)
        if limit is not None:
            child_links = child_links[:limit]

        semaphore = asyncio.Semaphore(rate_limit)
        child_locations = await asyncio.gather(
            *(
                fetch_child_locations(session, semaphore, child_url)
                for child_url in child_links
            )
        )

    merged_count = merge_child_locations(entries, list(child_locations))
    return entries, len(child_links), merged_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download TMC root and child pages, then write merged JSON.",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="Root page URL to crawl.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="JSON output path.",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=DEFAULT_RATE_LIMIT,
        help="Maximum number of child downloads to run at once.",
    )
    parser.add_argument(
        "--include-external",
        action="store_true",
        help="Also download child anchors outside the root URL's origin.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Download only the first N child links. Useful for quick checks.",
    )

    args = parser.parse_args()
    if args.rate_limit < 1:
        parser.error("--rate-limit must be at least 1")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    return args


def main() -> None:
    args = parse_args()
    try:
        entries, child_count, merged_count = winloop.run(
            build_tmc_data(
                url=args.url,
                rate_limit=args.rate_limit,
                include_external=args.include_external,
                limit=args.limit,
            )
        )
    except (aiohttp.ClientError, OSError, TimeoutError) as exc:
        raise SystemExit(f"Download failed: {exc}") from exc

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(entries, indent=4, ensure_ascii=False), encoding="utf-8"
    )

    print(f"Parsed {len(entries)} root entries.")
    print(f"Fetched {child_count} child pages.")
    print(f"Merged {merged_count} child pages.")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
