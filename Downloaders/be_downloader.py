import asyncio
import json
from collections.abc import Iterable
from typing import Any

import aiohttp
import winloop

from config import CONSTANTS
from Downloaders.base_downloader import BaseDownloader


class BEDownloader(BaseDownloader):
    """
    Downloader for Belgian highway camera data (Vlaams Verkeerscentrum).
    """

    CAMERA_API: str = CONSTANTS.BE.CAMERA_API
    EXCLUDED_API: str = CONSTANTS.BE.EXCLUDED_API
    DATA_API: str = CONSTANTS.BE.DATA_API

    def _get_http_settings(self) -> tuple[dict[str, str], aiohttp.ClientTimeout, aiohttp.TCPConnector]:
        headers, timeout, connector = super()._get_http_settings()
        headers.update(CONSTANTS.BE.REFERER_HEADER)
        return headers, timeout, connector

    async def get_data(self) -> list[dict[str, Any]]:
        """
        Downloads camera metadata and removes cameras disabled for streaming.
        """
        headers, timeout, connector = self._get_http_settings()
        async with aiohttp.ClientSession(
            headers=headers, timeout=timeout, connector=connector
        ) as session:
            cameras_raw, excluded_raw = await asyncio.gather(
                self.download(self.CAMERA_API, session),
                self.download(self.EXCLUDED_API, session),
            )

        cameras = json.loads(cameras_raw)
        excluded = json.loads(excluded_raw)
        disabled_ids = {str(camera_id) for camera_id in excluded.get("disabledCameras", [])}

        return [
            camera
            for camera in cameras
            if str(camera.get("nid")) not in disabled_ids
        ]

    async def get_popup_content(
        self,
        camera_id: str | int,
        session: aiohttp.ClientSession | None = None,
    ) -> str:
        return await self.download(f"{self.DATA_API}{camera_id}", session)

    async def get_popup_data(self, camera_ids: Iterable[str | int]) -> dict[str, str]:
        ids = [str(camera_id) for camera_id in camera_ids]
        headers, timeout, connector = self._get_http_settings()
        async with aiohttp.ClientSession(
            headers=headers, timeout=timeout, connector=connector
        ) as session:
            results = await asyncio.gather(
                *(self.get_popup_content(camera_id, session) for camera_id in ids),
                return_exceptions=True,
            )

        popups: dict[str, str] = {}
        for camera_id, result in zip(ids, results, strict=True):
            if isinstance(result, BaseException):
                print(f"Error downloading Belgium popup {camera_id}: {result}")
                continue
            popups[camera_id] = result
        return popups


if __name__ == "__main__":
    downloader = BEDownloader()
    print(winloop.run(downloader.get_data()))
