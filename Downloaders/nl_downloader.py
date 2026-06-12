import winloop

from config import CONSTANTS
from Downloaders.base_downloader import GenericDownloader

CAMERA_BASE_URL: str = CONSTANTS.NL.CAMERA_URL
CAMERA_API: str = CONSTANTS.UK.CAMERA_API_URL


class NLDownloader(GenericDownloader):
    """
    Downloader for the Netherlands highway camera data.
    """

    async def get_data(self):
        download_link: str = CAMERA_API
        return await self.download(download_link)


if __name__ == "__main__":
    downloader = NLDownloader()
    winloop.run(downloader.get_data())
