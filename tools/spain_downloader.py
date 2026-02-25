from base64 import b64decode

from config import CONSTANTS
from tools.utils import download_post, xor_decode

DATA_URL = CONSTANTS.SPAIN.CAMERA_API
XOR_KEY = CONSTANTS.SPAIN.XOR_KEY


def decode_data(camaras_data):
    try:
        decoded_bytes = b64decode(camaras_data, validate=True)
    except Exception as exc:
        raise ValueError(f"Base64 decode failed: {exc}") from exc

    json_text = xor_decode(decoded_bytes, XOR_KEY)

    print("Successfully downloaded camera data.")
    return json_text


async def get_spain_data():
    download_link = DATA_URL
    xored_data = await download_post(download_link)
    decoded_data = decode_data(xored_data)
    return decoded_data
