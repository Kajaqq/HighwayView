import asyncio
import winloop
import json
import aiohttp

from tools.france_asfa_deobfuscate import get_complete_url as get_asfa_url
from config import CONSTANTS
from tools.utils import unix_to_datetime
from Downloaders.base_downloader import BaseDownloader


class FranceDownloader(BaseDownloader):
    """
    Downloader for French highway camera data.
    Handles ASFA and Government (Bison Futé) sources separately.
    """

    async def get_gov_url(self, session: aiohttp.ClientSession) -> str | None:
        """
        Retrieves the government data URL by merging the timestamp with the data URL.

        Args:
            session (aiohttp.ClientSession): The active client session.

        Returns:
            str | None: The formatted URL for the government camera data, or None if failed.
        """
        base_url: str = CONSTANTS.FRANCE.BASE_URL
        timestamp_url = f"{base_url}{CONSTANTS.FRANCE.TIMESTAMP_URL}"
        camera_url = f"{base_url}{CONSTANTS.FRANCE.CAMERA_API}"

        try:
            timestamp_raw: str = await self.download(url=timestamp_url, session=session)
            timestamp: int = json.loads(timestamp_raw)[0]

            if isinstance(timestamp, str):
                timestamp = int(timestamp)

            timestamp_formatted: str = unix_to_datetime(timestamp)
            return camera_url.format(datetime=timestamp_formatted)
        except (ValueError, IndexError, Exception) as e:
            print(f"Error fetching/parsing timestamp: {e}")
            return None

    async def download_asfa(self, session: aiohttp.ClientSession) -> str:
        """
        Downloads the ASFA camera data.

        Args:
            session (aiohttp.ClientSession): The active client session.

        Returns:
            str: The raw ASFA data.
        """
        asfa_camera_url: str = await get_asfa_url()
        return await self.download(url=asfa_camera_url, session=session)

    async def download_gov(self, session: aiohttp.ClientSession) -> str | None:
        """
        Downloads the Government GeoJSON camera data.

        Args:
            session (aiohttp.ClientSession): The active client session.

        Returns:
            str | None: The raw Government data or None if URL retrieval fails.
        """
        gov_camera_url: str | None = await self.get_gov_url(session=session)
        if not gov_camera_url:
            return None
        return await self.download(url=gov_camera_url, session=session)

    async def get_data(self, asfa_only: bool = False, gov_only: bool = False) -> tuple[str | None, str | None]:
        """
        Orchestrates downloading data from ASFA, Government, or both sources.

        Args:
            asfa_only (bool, optional): If True, downloading is restricted to ASFA only.
                Defaults to False.
            gov_only (bool, optional): If True, downloading is restricted to Gov only.
                Defaults to False.

        Returns:
            tuple[str | None, str | None]: A tuple containing the raw ASFA string
                and raw Government string respectively. Values will be None if not fetched.
        """
        headers, timeout, connector = self._get_http_settings()
        asfa_camera_data: str | None = None
        gov_camera_data: str | None = None

        async with aiohttp.ClientSession(
            headers=headers, connector=connector, timeout=timeout
        ) as session:
            fetch_asfa: bool = asfa_only or (not gov_only)
            fetch_gov: bool = gov_only or (not asfa_only)

            asfa_task = (
                self.download_asfa(session)
                if fetch_asfa
                else asyncio.sleep(0, result=None)
            )
            gov_task = (
                self.download_gov(session)
                if fetch_gov
                else asyncio.sleep(0, result=None)
            )

            asfa_camera_data, gov_camera_data = await asyncio.gather(
                asfa_task, gov_task
            )

        return asfa_camera_data, gov_camera_data


if __name__ == "__main__":
    asfa_only = False
    gov_only = False
    downloader = FranceDownloader()
    winloop.run(downloader.get_data(asfa_only, gov_only))
