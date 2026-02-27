import winloop
from config import CONSTANTS
from Downloaders.base_downloader import BaseDownloader

CAMERA_BASE_URL = CONSTANTS.UK.CAMERA_URL
CAMERA_API = CONSTANTS.UK.CAMERA_API_URL


class UKDownloader(BaseDownloader):
    async def get_data(self):
        download_link = CAMERA_API
        return await self.download(download_link)


async def get_uk_data():
    downloader = UKDownloader()
    return await downloader.get_data()


if __name__ == "__main__":
    winloop.run(get_uk_data())
