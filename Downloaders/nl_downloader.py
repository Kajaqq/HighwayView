import winloop
from config import CONSTANTS
from Downloaders.base_downloader import GenericDownloader


class NLDownloader(GenericDownloader):
    CAMERA_API: str = CONSTANTS.NL.CAMERA_API
    """
    Downloader for the Netherlands highway camera data.
    """

    async def get_data(self):
        download_link: str = self.CAMERA_API
        return await self.download(download_link)



if __name__ == "__main__":
    downloader = NLDownloader()
    print(winloop.run(downloader.get_data()))
