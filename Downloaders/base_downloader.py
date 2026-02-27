import socket
from abc import ABC, abstractmethod

import aiohttp

from config import CONSTANTS


class HTTPError(Exception):
    """Custom exception for HTTP errors"""

    pass


class BaseDownloader(ABC):
    def __init__(
        self,
        timeout_int=CONSTANTS.COMMON.HTTP_TIMEOUT,
        rate_limit=CONSTANTS.COMMON.RATE_LIMIT,
    ):
        self.timeout_int = timeout_int
        self.rate_limit = rate_limit

    def _get_http_settings(self):
        headers = CONSTANTS.COMMON.DEFAULT_HEADERS.copy()
        timeout = aiohttp.ClientTimeout(total=self.timeout_int)
        resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"])
        connector = aiohttp.TCPConnector(
            resolver=resolver,
            limit=self.rate_limit,
            ttl_dns_cache=300,
            family=socket.AF_INET,
        )
        return headers, timeout, connector

    @staticmethod
    def _format_error_message(method: str, url: str, error: Exception) -> str:
        method = method.upper()
        return f"{method} request failed for {url}: {error}"

    @staticmethod
    async def _async_request(
        session: aiohttp.ClientSession, method: str, url: str, return_type: str = "text"
    ) -> tuple[bytes, int] | str:
        async with session.request(method, url) as response:
            response.raise_for_status()
            if return_type == "bytes":
                return await response.read(), response.status
            else:
                return await response.text()

    async def _fetch_response(
        self,
        url: str,
        method: str,
        session: aiohttp.ClientSession | None,
    ) -> str:
        try:
            if session is None:
                headers, timeout_ctx, connector = self._get_http_settings()
                async with aiohttp.ClientSession(
                    headers=headers, timeout=timeout_ctx, connector=connector
                ) as new_session:
                    return await self._async_request(new_session, method, url)
            else:
                return await self._async_request(session, method, url)
        except aiohttp.ClientError as e:
            raise HTTPError(self._format_error_message(method, url, e)) from e

    async def download(
        self, url: str, session: aiohttp.ClientSession | None = None
    ) -> str:
        """Download content from URL with proper error handling and timeout"""
        return await self._fetch_response(url, "GET", session)

    async def download_post(
        self, url: str, session: aiohttp.ClientSession | None = None
    ) -> str:
        """Download content via POST with proper error handling and timeout"""
        return await self._fetch_response(url, "POST", session)

    @abstractmethod
    async def get_data(self):
        """Abstract method to be implemented by child classes"""
        pass


class GenericDownloader(BaseDownloader):
    """A generic downloader that implements BaseDownloader for when just the HTTP features are needed."""

    async def get_data(self):
        pass
