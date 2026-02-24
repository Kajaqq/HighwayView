from tools.utils import download
from config import CONSTANTS

CAMERA_BASE_URL = CONSTANTS.UK.CAMERA_URL
CAMERA_API = CONSTANTS.UK.CAMERA_API_URL

async def get_uk_data():
    download_link = CAMERA_API
    return await download(download_link)