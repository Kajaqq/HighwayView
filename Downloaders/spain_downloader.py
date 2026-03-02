import winloop
from base64 import b64decode

from config import CONSTANTS
from tools.utils import xor_decode
from Downloaders.base_downloader import BaseDownloader

DATA_URL = CONSTANTS.SPAIN.CAMERA_API
XOR_KEY = CONSTANTS.SPAIN.XOR_KEY


class SpainDownloader(BaseDownloader):
    def decode_data(self, camaras_data):
        try:
            decoded_bytes = b64decode(camaras_data, validate=True)
        except Exception as exc:
            raise ValueError(f"Base64 decode failed: {exc}") from exc

        json_text = xor_decode(decoded_bytes, XOR_KEY)

        print("Successfully downloaded camera data.")
        return json_text

    async def get_data(self):
        download_link = DATA_URL
        xored_data = await self.download_post(download_link)
        decoded_data = self.decode_data(xored_data)
        return decoded_data


if __name__ == "__main__":
    downloader = SpainDownloader()
    winloop.run(downloader.get_data())
