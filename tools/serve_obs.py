from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
import winloop
from aiohttp import ClientError, ClientSession, web

from config import CONSTANTS
from get_datex import COUNTRY_CONFIGS
from tools.create_camera_loop import HIGHWAY_SEQUENCES
from tools.create_camera_loop import main as create_camera_loop
from tools.create_html import generate_html, get_camera_urls
from tools.utils import create_url, load_json

LOGGER = logging.getLogger(__name__)
DATA_DIR = CONSTANTS.COMMON.DATA_DIR
COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
OVERLAY_ASSETS = frozenset(
    {"index.html", "styles.css", "overlay.js", "overlay_data.json", "overlay_data.js"}
)
NO_STORE_HEADERS = {"Cache-Control": "no-store"}
PROXY_CACHE_HEADERS = {"Cache-Control": "no-store, no-cache, must-revalidate"}
NL_ASSET_BASE_URL = "https://img.inmoves.nl/"
NL_ASSET_PROXY_PREFIX = "/proxy/cameras/NL/assets/"
NL_STREAM_PROXY_PREFIX = "/proxy/streams/NL/"
NL_STREAM_REFERER_HEADER = {"Referer": CONSTANTS.NL.CAMERA_URL}
PROXY_RESPONSE_HEADERS = ("Content-Type", "Accept-Ranges", "Content-Range")


@contextlib.asynccontextmanager
async def proxy_session_context(app: web.Application) -> AsyncIterator[None]:
    resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])
    timeout = aiohttp.ClientTimeout(total=CONSTANTS.COMMON.HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(
        resolver=resolver,
        ttl_dns_cache=300,
        family=socket.AF_INET,
    )
    async with ClientSession(
            headers=CONSTANTS.COMMON.DEFAULT_HEADERS,
            connector=connector,
            timeout=timeout,
    ) as session:
        app["proxy_session"] = session
        yield


def _interval_seconds(request: web.Request) -> int:
    raw_value = request.query.get("interval")
    if raw_value is None:
        return DEFAULT_INTERVAL

    try:
        interval = int(raw_value)
    except ValueError:
        raise web.HTTPBadRequest(
            text="Query parameter interval must be an integer."
        ) from None

    if interval <= 0:
        raise web.HTTPBadRequest(text="Query parameter interval must be positive.")
    return interval


def _proxy_response_headers(
        response: aiohttp.ClientResponse, *, include_content_length: bool = False
) -> dict[str, str]:
    headers = {
        header: response.headers[header]
        for header in PROXY_RESPONSE_HEADERS
        if header in response.headers
    }
    if include_content_length and "Content-Length" in response.headers:
        headers["Content-Length"] = response.headers["Content-Length"]
    return {**headers, **PROXY_CACHE_HEADERS}


def _rewrite_nl_stream_url(upstream_url: str) -> str:
    parsed_url = urlparse(upstream_url)
    if parsed_url.netloc != "stream.inmoves.nl":
        return upstream_url

    proxy_url = f"{NL_STREAM_PROXY_PREFIX}{parsed_url.path.lstrip('/')}"
    if parsed_url.query:
        proxy_url = f"{proxy_url}?{parsed_url.query}"
    return proxy_url


def _rewrite_nl_hls_playlist(body: bytes, upstream_url: str) -> bytes:
    text = body.decode("utf-8")
    rewritten_lines: list[str] = []

    for line in text.splitlines():
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            rewritten_lines.append(line)
            continue

        rewritten_lines.append(
            _rewrite_nl_stream_url(urljoin(upstream_url, stripped_line))
        )

    rewritten_text = "\n".join(rewritten_lines)
    if text.endswith("\n"):
        rewritten_text = f"{rewritten_text}\n"
    return rewritten_text.encode("utf-8")


def _rewrite_nl_proxy_body(body: bytes) -> bytes:
    return (
        body.replace(NL_ASSET_BASE_URL.encode(), NL_ASSET_PROXY_PREFIX.encode())
        .replace(b"http://img.inmoves.nl/", NL_ASSET_PROXY_PREFIX.encode())
        .replace(b"//img.inmoves.nl/", NL_ASSET_PROXY_PREFIX.encode())
        .replace(CONSTANTS.NL.CAMERA_URL.encode(), NL_STREAM_PROXY_PREFIX.encode())
        .replace(b"http://stream.inmoves.nl/", NL_STREAM_PROXY_PREFIX.encode())
        .replace(b"//stream.inmoves.nl/", NL_STREAM_PROXY_PREFIX.encode())
    )


# noinspection PyUnusedLocal
async def healthz(request: web.Request) -> web.Response:
    return web.json_response({"ok": True}, headers=NO_STORE_HEADERS)


async def cameras(request: web.Request) -> web.Response:
    country = request.match_info["country"].upper()
    if country not in COUNTRY_MAP:
        raise web.HTTPNotFound(text=f"Unknown camera country: {country}.")

    camera_file = DATA_DIR / f"cameras_{country.lower()}_online.json"
    if not camera_file.exists():
        raise web.HTTPNotFound(text=f"Camera data not found: {camera_file}.")

    try:
        json_data = load_json(camera_file)
        camera_ids = (
            create_camera_loop(json_data)
            if COUNTRY_MAP[country] in HIGHWAY_SEQUENCES
            else None
        )
        camera_urls, detected_country = get_camera_urls(
            json_data=json_data,
            camera_ids=camera_ids,
        )
    except OSError as e:
        raise web.HTTPInternalServerError(text=str(e)) from e
    except ValueError as e:
        raise web.HTTPBadRequest(text=str(e)) from e

    if not camera_urls:
        raise web.HTTPNotFound(text="No cameras found.")

    if detected_country == "NL":
        proxied_camera_urls = []
        for camera_id, url, highway_name, camera_number, media_type in camera_urls:
            proxy_url = str(
                request.app.router["camera_proxy"].url_for(camera_id=camera_id)
            )
            if query := urlparse(url).query:
                proxy_url = f"{proxy_url}?{query}"
            proxied_camera_urls.append(
                (camera_id, proxy_url, highway_name, camera_number, media_type)
            )
        camera_urls = proxied_camera_urls

    return web.Response(
        text=generate_html(camera_urls, _interval_seconds(request), detected_country),
        content_type="text/html",
        charset="utf-8",
        headers=NO_STORE_HEADERS,
    )


async def camera_proxy(request: web.Request) -> web.Response:
    camera_id = request.match_info["camera_id"]
    try:
        upstream_url, _ext = create_url("NL", camera_id, "iframe")
        if request.query_string:
            upstream_url = f"{upstream_url}?{request.query_string}"
    except ValueError as e:
        raise web.HTTPBadRequest(text=str(e)) from e

    session: ClientSession = request.app["proxy_session"]
    LOGGER.info("NL proxy request camera_id=%s upstream=%s", camera_id, upstream_url)
    try:
        async with session.get(
                upstream_url,
                allow_redirects=True,
                headers=CONSTANTS.NL.REFERER_HEADER,
        ) as response:
            body = await response.read()
            content_type = response.headers.get("Content-Type", "text/html")
            if content_type.startswith(("text/html", "application/xhtml+xml")):
                body = _rewrite_nl_proxy_body(body)
            LOGGER.info(
                "NL upstream response camera_id=%s status=%s content_type=%s "
                "bytes=%s final_url=%s",
                camera_id,
                response.status,
                content_type,
                len(body),
                response.url,
            )
            if response.status >= 400:
                LOGGER.warning(
                    "NL upstream error camera_id=%s upstream=%s body=%r",
                    camera_id,
                    upstream_url,
                    body[:500].decode("utf-8", errors="replace"),
                )
                raise web.HTTPBadGateway(
                    text=f"NL camera upstream returned HTTP {response.status}."
                )

            return web.Response(
                body=body,
                status=response.status,
                headers={
                    "Content-Type": content_type,
                    **PROXY_CACHE_HEADERS,
                },
            )
    except TimeoutError as e:
        LOGGER.warning(
            "NL upstream timed out camera_id=%s upstream=%s",
            camera_id,
            upstream_url,
        )
        raise web.HTTPGatewayTimeout(text="NL camera upstream timed out.") from e
    except ClientError as e:
        LOGGER.warning(
            "NL upstream request failed camera_id=%s upstream=%s error=%s",
            camera_id,
            upstream_url,
            e,
        )
        raise web.HTTPBadGateway(text=f"NL camera upstream request failed: {e}") from e


async def nl_asset_proxy(request: web.Request) -> web.Response:
    asset_path = request.match_info["asset_path"]
    if not asset_path:
        raise web.HTTPNotFound(text="Missing NL asset path.")

    upstream_url = f"{NL_ASSET_BASE_URL}{asset_path}"
    if request.query_string:
        upstream_url = f"{upstream_url}?{request.query_string}"

    session: ClientSession = request.app["proxy_session"]
    LOGGER.info("NL asset proxy request upstream=%s", upstream_url)
    try:
        async with session.get(
                upstream_url,
                allow_redirects=True,
                headers=NL_STREAM_REFERER_HEADER,
        ) as response:
            body = await response.read()
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            if "javascript" in content_type.lower():
                body = _rewrite_nl_proxy_body(body)

            return web.Response(
                body=body,
                status=response.status,
                headers=_proxy_response_headers(response),
            )
    except TimeoutError as e:
        LOGGER.warning("NL asset upstream timed out upstream=%s", upstream_url)
        raise web.HTTPGatewayTimeout(text="NL asset upstream timed out.") from e
    except ClientError as e:
        LOGGER.warning("NL asset upstream request failed upstream=%s error=%s", upstream_url, e)
        raise web.HTTPBadGateway(text=f"NL asset upstream request failed: {e}") from e


async def nl_stream_proxy(request: web.Request) -> web.Response:
    stream_path = request.match_info["stream_path"]
    if not stream_path:
        raise web.HTTPNotFound(text="Missing NL stream path.")

    upstream_url = f"{CONSTANTS.NL.CAMERA_URL}{stream_path}"
    if request.query_string:
        upstream_url = f"{upstream_url}?{request.query_string}"

    request_headers = dict(NL_STREAM_REFERER_HEADER)
    if range_header := request.headers.get("Range"):
        request_headers["Range"] = range_header

    session: ClientSession = request.app["proxy_session"]
    LOGGER.info("NL stream proxy request upstream=%s", upstream_url)
    try:
        async with session.get(
                upstream_url,
                allow_redirects=True,
                headers=request_headers,
        ) as response:
            body = await response.read()
            content_type = response.headers.get("Content-Type", "application/octet-stream")
            content_type_lower = content_type.lower()
            is_hls_playlist = (
                    "mpegurl" in content_type_lower
                    or stream_path.endswith((".m3u8", ".m3u"))
            )
            if response.status < 400 and is_hls_playlist:
                body = _rewrite_nl_hls_playlist(body, upstream_url)

            return web.Response(
                body=body,
                status=response.status,
                headers=_proxy_response_headers(
                    response,
                    include_content_length=not is_hls_playlist,
                ),
            )
    except TimeoutError as e:
        LOGGER.warning("NL stream upstream timed out upstream=%s", upstream_url)
        raise web.HTTPGatewayTimeout(text="NL stream upstream timed out.") from e
    except ClientError as e:
        LOGGER.warning("NL stream upstream request failed upstream=%s error=%s", upstream_url, e)
        raise web.HTTPBadGateway(text=f"NL stream upstream request failed: {e}") from e


async def overlay_redirect(request: web.Request) -> web.Response:
    country = request.match_info["country"].upper()
    if country not in COUNTRY_CONFIGS:
        raise web.HTTPNotFound(text=f"Unknown alert overlay country: {country}.")
    raise web.HTTPMovedPermanently(location=f"/overlay/{country}/")


async def overlay_asset(request: web.Request) -> web.FileResponse:
    country = request.match_info["country"].upper()
    if country not in COUNTRY_CONFIGS:
        raise web.HTTPNotFound(text=f"Unknown alert overlay country: {country}.")

    asset = request.match_info.get("asset", "index.html")
    if asset not in OVERLAY_ASSETS:
        raise web.HTTPNotFound(text=f"Unknown overlay asset: {asset}.")

    overlay_dir = DATA_DIR / Path(f"overlay_{COUNTRY_MAP[country].lower()}")
    asset_file = overlay_dir / asset
    if not asset_file.exists():
        raise web.HTTPNotFound(text=f"Overlay asset not found: {asset_file}.")
    return web.FileResponse(asset_file, headers=NO_STORE_HEADERS)


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/cameras/{country}", cameras)
    app.router.add_get(
        "/proxy/cameras/NL/{camera_id}/embed", camera_proxy, name="camera_proxy"
    )
    app.router.add_get("/proxy/cameras/NL/assets/{asset_path:.*}", nl_asset_proxy)
    app.router.add_get("/proxy/streams/NL/{stream_path:.*}", nl_stream_proxy)
    app.router.add_get("/overlay/{country}", overlay_redirect)
    app.router.add_get("/overlay/{country}/", overlay_asset)
    app.router.add_get("/overlay/{country}/{asset}", overlay_asset)
    app.cleanup_ctx.append(proxy_session_context)
    return app


async def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
    app = create_app()
    runner = web.AppRunner(app)

    await runner.setup()
    site = web.TCPSite(runner, DEFAULT_HOST, DEFAULT_PORT)
    try:
        await site.start()
        base_url = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
        print(f"Serving OBS artifacts at {base_url}")
        print(f"Health check: {base_url}/healthz")
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    winloop.run(main())
