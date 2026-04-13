import winloop
from base64 import b64decode

from config import CONSTANTS
from tools.utils import xor_decode
from Downloaders.base_downloader import BaseDownloader

DATA_URL: str = CONSTANTS.SPAIN.CAMERA_API
XOR_KEY: str = CONSTANTS.SPAIN.XOR_KEY


class SpainDownloader(BaseDownloader):
    """
    Downloader for Spanish government highway camera data (DGT).
    """

    @staticmethod
    def decode_data(camaras_data: str | bytes) -> str:
        """
        Decoder for the base64 encoded and XOR-obfuscated camera data.

        Args:
            camaras_data (str | bytes): The raw obfuscated data.

        Raises:
            ValueError: If base64 decoding fails.

        Returns:
            str: The decoded JSON string.
        """
        try:
            decoded_bytes: bytes = b64decode(camaras_data, validate=True)
        except Exception as exc:
            raise ValueError(f"Base64 decode failed: {exc}") from exc

        json_text: str = xor_decode(decoded_bytes, XOR_KEY)

        print("Successfully downloaded camera data.")
        return json_text

    async def get_data(self) -> str:
        """
        Downloads the data from the government API and decodes it.

        Returns:
            str: The decoded JSON data as a string.
        """
        download_link: str = DATA_URL
        raw_data: str = await self.download_post(download_link)
        decoded_data: str = self.decode_data(raw_data)
        return decoded_data


if __name__ == "__main__":
    downloader = SpainDownloader()
    winloop.run(downloader.get_data())
