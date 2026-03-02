import asyncio
import winloop
import json
import aiohttp

from tools.france_asfa_deobfuscate import get_complete_url as get_asfa_url
from config import CONSTANTS
from tools.utils import unix_to_datetime
from Downloaders.base_downloader import BaseDownloader


class FranceDownloader(BaseDownloader):
    async def get_gov_url(self, session):
        base_url = CONSTANTS.FRANCE.BASE_URL
        timestamp_url = f"{base_url}{CONSTANTS.FRANCE.TIMESTAMP_URL}"
        camera_url = f"{base_url}{CONSTANTS.FRANCE.CAMERA_API}"

        try:
            timestamp_raw = await self.download(url=timestamp_url, session=session)
            timestamp = json.loads(timestamp_raw)[0]

            if isinstance(timestamp, str):
                timestamp = int(timestamp)

            timestamp_formatted = unix_to_datetime(timestamp)
            return camera_url.format(datetime=timestamp_formatted)
        except (ValueError, IndexError, Exception) as e:
            print(f"Error fetching/parsing timestamp: {e}")
            return None

    async def download_asfa(self, session):
        asfa_camera_url = await get_asfa_url()
        return await self.download(url=asfa_camera_url, session=session)

    async def download_gov(self, session):
        gov_camera_url = await self.get_gov_url(session=session)
        if not gov_camera_url:
            return None
        return await self.download(url=gov_camera_url, session=session)

    async def get_data(self, asfa_only=False, gov_only=False):
        headers, timeout, connector = self._get_http_settings()
        asfa_camera_data = None
        gov_camera_data = None
        async with aiohttp.ClientSession(
            headers=headers, connector=connector, timeout=timeout
        ) as session:
            fetch_asfa = asfa_only or (not gov_only)
            fetch_gov = gov_only or (not asfa_only)

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
