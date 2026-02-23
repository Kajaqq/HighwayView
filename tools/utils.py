import asyncio
import datetime
import json
import math
import socket
from itertools import cycle
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
from lambert import Lambert93, convertToWGS84Deg

from config import CONSTANTS

EARTH_RADIUS_KM = CONSTANTS.COMMON.EARTH_RADIUS_KM
DEFAULT_HEADERS = CONSTANTS.COMMON.DEFAULT_HEADERS


class HTTPError(Exception):
    """Custom exception for HTTP errors"""

    pass


def get_http_settings(
    timeout_int=CONSTANTS.COMMON.HTTP_TIMEOUT, rate_limit=CONSTANTS.COMMON.RATE_LIMIT
):
    headers = DEFAULT_HEADERS.copy()
    timeout = aiohttp.ClientTimeout(total=timeout_int)
    resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])
    connector = aiohttp.TCPConnector(
        resolver=resolver, limit=rate_limit, ttl_dns_cache=300, family=socket.AF_INET
    )
    return headers, timeout, connector


def format_error_message(method: str, url: str, error: Exception) -> str:
    method = method.upper()
    return f"{method} request failed for {url}: {error}"


async def async_request(
    session: aiohttp.ClientSession, method: str, url: str, return_type: str = "text"
) -> tuple[bytes, int] | str:
    async with session.request(method, url) as response:
        response.raise_for_status()
        if return_type == "bytes":
            return await response.read(), response.status
        else:
            return await response.text()


async def fetch_response(
    url: str,
    method: str,
    http_timeout: float,
    session: aiohttp.ClientSession | None,
) -> str:
    try:
        if session is None:
            headers, timeout_ctx, connector = get_http_settings(
                timeout_int=http_timeout
            )
            async with aiohttp.ClientSession(
                headers=headers, timeout=timeout_ctx, connector=connector
            ) as new_session:
                return await async_request(new_session, method, url)
        else:
            return await async_request(session, method, url)
    except aiohttp.ClientError as e:
        raise HTTPError(format_error_message(method, url, e)) from e


async def download(
    url: str,
    http_timeout: float = CONSTANTS.COMMON.HTTP_TIMEOUT,
    session: aiohttp.ClientSession | None = None,
) -> str:
    """Download content from URL with proper error handling and timeout"""
    return await fetch_response(url, "GET", http_timeout, session)


async def download_post(
    url: str,
    http_timeout: float = CONSTANTS.COMMON.HTTP_TIMEOUT,
    session: aiohttp.ClientSession | None = None,
) -> str:
    """Download content via POST with proper error handling and timeout"""
    return await fetch_response(url, "POST", http_timeout, session)


def check_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


async def check_parent_dir_async(path: Path) -> None:
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)


def check_json(json_data, indent: int | None) -> str:
    if isinstance(json_data, str):
        return json_data
    try:
        return json.dumps(json_data, ensure_ascii=False, indent=indent)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not serializable to JSON: {e}") from e


def save_json(json_data, output: Path) -> None:
    """Save JSON data to a file with proper error handling"""
    output = Path(output)
    check_parent_dir(output)
    try:
        content = check_json(json_data, indent=4)
        output.write_text(content, encoding="utf-8")
    except OSError as e:
        raise OSError(f"Failed to write file {output}: {e}") from e


async def save_json_async(json_data, output: Path) -> None:
    """Save JSON data to a file with proper error handling"""
    output = Path(output)
    await check_parent_dir_async(output)
    try:
        content = await asyncio.to_thread(check_json, json_data, indent=None)
        async with aiofiles.open(output, "w", encoding="utf-8") as outfile:
            await outfile.write(content)
    except OSError as e:
        raise OSError(f"Failed to write file {output}: {e}") from e


def load_json(json_data: Path | str | list | dict) -> Any:
    """Load JSON data from a file, raw string, or return if already a dict/list"""
    if isinstance(json_data, (list, dict)):
        return json_data
    try:
        if isinstance(json_data, Path):
            with Path.open(json_data, encoding="utf-8") as infile:
                return json.load(infile)
        else:
            return json.loads(json_data)
    except OSError as e:
        raise OSError(f"Failed to read file {json_data}: {e}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON data: {e}") from e


def create_url(base, camera_id, camera_type) -> tuple[str, str | None]:
    if base == "FR":
        if camera_type == "asfa_vid":
            base_url = CONSTANTS.FRANCE.ASFA.CAMERA_URL
            ext = CONSTANTS.FRANCE.ASFA.VIDEO_EXT
            return base_url.format(camera_id=camera_id), ext
        base_url = CONSTANTS.FRANCE.CAMERA_URL
        ext = {
            "vid": CONSTANTS.FRANCE.VIDEO_EXT,
            "img": CONSTANTS.FRANCE.IMAGE_EXT,
        }.get(camera_type)
        return f"{base_url}{camera_id}{ext}", ext
    if base == "ES":
        base_url = CONSTANTS.SPAIN.CAMERA_URL
        ext = CONSTANTS.SPAIN.IMAGE_EXT
        return f"{base_url}{camera_id}{ext}", ext

    else:
        raise ValueError("Invalid data")


def unix_to_datetime(timestamp: int | float | str, tz=CONSTANTS.FRANCE.PARIS_TZ) -> str:
    """Convert Unix timestamp to datetime with timezone normalization"""
    timestamp_len = len(str(timestamp))
    if timestamp_len > 10:
        zeros_to_remove = timestamp_len - 10
        divisor = 10**zeros_to_remove
        normalized_timestamp = float(timestamp) / divisor
    else:
        normalized_timestamp = float(timestamp)
    dt = datetime.datetime.fromtimestamp(normalized_timestamp, tz)
    return dt.strftime("%Y%m%d-%H%M%S")


def xor_decode(msg: bytes, key_str: str) -> str:
    """XOR decode message with a given key"""
    key = key_str.encode("utf-8")
    decoded = bytearray(b ^ k for b, k in zip(msg, cycle(key)))
    return decoded.decode("utf-8")


def convert_to_wgs84(lon: float, lat: float) -> tuple[float, float]:
    pt = convertToWGS84Deg(lon, lat, Lambert93)
    return pt.getX(), pt.getY()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points on Earth in kilometers."""
    r = EARTH_RADIUS_KM
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    # Clamp value to 1.0 to handle floating-point errors (prevents ValueError in asin)
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))
