import socket
from abc import ABC, abstractmethod
from typing import Any

import aiohttp

from config import CONSTANTS


class HTTPError(Exception):
    """Custom exception for HTTP errors."""

    pass


class BaseDownloader(ABC):
    """
    Abstract base class for all data downloaders.

    Provides common functionality for creating HTTP sessions with standardized
    timeout, rate limiting, and error handling configurations.
    """

    def __init__(
        self,
        timeout_int: float = CONSTANTS.COMMON.HTTP_TIMEOUT,
        rate_limit: int = CONSTANTS.COMMON.RATE_LIMIT,
    ) -> None:
        """
        Initializes the Downloader.

        Args:
            timeout_int (float, optional): The HTTP request timeout in seconds.
                Defaults to CONSTANTS.COMMON.HTTP_TIMEOUT -> 20s.
            rate_limit (int, optional): The maximum number of concurrent connections.
                Defaults to CONSTANTS.COMMON.RATE_LIMIT -> 50 requests.
        """
        self.timeout_int = timeout_int
        self.rate_limit = rate_limit

    def _get_http_settings(
        self,
    ) -> tuple[dict[str, str], aiohttp.ClientTimeout, aiohttp.TCPConnector]:
        """
        Generates the standard HTTP settings for a session.

        Returns:
            tuple[dict[str, str], aiohttp.ClientTimeout, aiohttp.TCPConnector]:
                A tuple containing the headers dictionary, timeout context, and TCP connector.
        """

        headers: dict[str, str] = CONSTANTS.COMMON.DEFAULT_HEADERS.copy()
        timeout = aiohttp.ClientTimeout(total=self.timeout_int)

        # This shouldn't be required but for some reason it is
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
        """
        Formats an error message to include the HTTP method and URL.

        Args:
            method (str): The HTTP method used (e.g., 'GET', 'POST').
            url (str): The URL that failed.
            error (Exception): The exception caught.

        Returns:
            str: The formatted error message.
        """
        method = method.upper()
        return f"{method} request failed for {url}: {error}"

    @staticmethod
    async def _async_request(
        session: aiohttp.ClientSession, method: str, url: str, return_type: str = "text"
    ) -> tuple[bytes, int] | str:
        """
        Executes an asynchronous HTTP request.

        Args:
            session (aiohttp.ClientSession): The active client session.
            method (str): The HTTP method (e.g., 'GET', 'POST').
            url (str): The target URL.
            return_type (str, optional): The expected return type ('bytes' for images or 'text' for everything else). Defaults to 'text'.

        Returns:
            tuple[bytes, int] | str: The response content. Either a tuple of (bytes, status code)
                or a string depending on return_type.
        """
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
        """
        Fetches the response from a URL using an existing or new session.

        Args:
            url (str): The target URL.
            method (str): The HTTP method.
            session (aiohttp.ClientSession | None): An existing session or None if a new session should be created.

        Raises:
            HTTPError: If the request fails due to an aiohttp.ClientError.

        Returns:
            str: The raw text response.
        """
        try:
            if session is None:
                headers, timeout_ctx, connector = self._get_http_settings()
                async with aiohttp.ClientSession(
                    headers=headers, timeout=timeout_ctx, connector=connector
                ) as new_session:  # Create a new session
                    content = await self._async_request(new_session, method, url)
                    return str(content)  # enforce return type as str
            else:
                content = await self._async_request(
                    session, method, url
                )  # Use existing session
                return str(content)
        except aiohttp.ClientError as e:
            raise HTTPError(self._format_error_message(method, url, e)) from e

    async def get_settings(
        self,
    ) -> tuple[dict[str, str], aiohttp.ClientTimeout, aiohttp.TCPConnector]:
        """
        Public method to get standard HTTP settings.

        Returns:
            tuple[dict[str, str], aiohttp.ClientTimeout, aiohttp.TCPConnector]:
                The headers, timeout context, and TCP connector.
        """
        return self._get_http_settings()

    async def download(
        self, url: str, session: aiohttp.ClientSession | None = None
    ) -> str:
        """
        Public method to download content from a URL via a GET request.

        Args:
            url (str): The target URL.
            session (aiohttp.ClientSession | None, optional): An active session. Defaults to None.

        Returns:
            str: The downloaded content as a string.
        """
        return await self._fetch_response(url, "GET", session)

    async def download_post(
        self, url: str, session: aiohttp.ClientSession | None = None
    ) -> str:
        """
        Public method to download content from a URL via a POST request.

        Args:
            url (str): The target URL.
            session (aiohttp.ClientSession | None, optional): An active session. Defaults to None.

        Returns:
            str: The downloaded content as a string.
        """
        return await self._fetch_response(url, "POST", session)

    @abstractmethod
    async def get_data(self) -> Any:
        """
        Abstract method for raw data retrieval. Must be implemented by subclasses.

        Returns:
            Any: The raw data.
        """
        pass


class GenericDownloader(BaseDownloader):
    """
    A generic downloader that implements BaseDownloader for when just the HTTP
    features are needed.
    """

    async def get_data(self) -> None:
        """
        No-op implementation of get_data for GenericDownloader.
        """
        pass
