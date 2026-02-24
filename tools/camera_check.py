import asyncio
from pathlib import Path
import aiofiles
import aiohttp
import winloop
from tqdm.asyncio import tqdm

from tools.utils import load_json, create_url, get_http_settings, save_json
import tools.diff_hash as diff_hash
from config import CONSTANTS

SEP = CONSTANTS.COMMON.SEPARATOR
DEFAULT_RATE_LIMIT = CONSTANTS.COMMON.RATE_LIMIT
JSON_OUTPUT_DIR = CONSTANTS.COMMON.DATA_DIR
IMAGE_DIR = CONSTANTS.COMMON.IMG_DIR

winloop.install()

async def save_image(camera_id, ext, img_bytes, output_dir=IMAGE_DIR):
    filename = f"{camera_id}{ext}"
    file_path = Path.joinpath(output_dir, filename)
    async with aiofiles.open(file_path, mode="wb") as f:
        await f.write(img_bytes)


def get_camera_data(json_data: dict):
    country = json_data[0]["highway"]["country"]
    if country == "IT":
        camera_ids = [
            [camera["camera_id"], camera["url"]]
            for highway in json_data
            for camera in highway["highway"]["cameras"]
        ]
    else:
        camera_ids = [
            [camera["camera_id"], camera["camera_type"]]
            for highway in json_data
            for camera in highway["highway"]["cameras"]
        ]
    return [country, camera_ids]


async def check_camera_async(
    client, source, camera_id, camera_type, rate_limiter, download, output_dir=None
):
    if source != "IT":
        url, ext = create_url(source, camera_id, camera_type)
    else:
        url = camera_type  # Special case for Italy where urls are in the data directly
        ext = CONSTANTS.ITALY.VIDEO_EXT
    async with rate_limiter:
        response_bytes = b""
        try:
            async with client.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                response_bytes = await response.read()
                status_code = response.status
            if len(response_bytes) < 1000:
                raise aiohttp.ClientPayloadError(  # noqa: TRY301
                    f"Response too small: {len(response_bytes)} bytes"
                )
            if not download:
                return {"id": camera_id, "status": status_code}
            else:
                await save_image(camera_id, ext, response_bytes, output_dir)
                return {"id": camera_id, "status": status_code}  # noqa: TRY300

        except (TimeoutError, aiohttp.ClientError, aiohttp.ClientPayloadError):
            return {"id": camera_id, "status": False, "len": len(response_bytes)}


def remove_offline_cameras(camera_json, errored_cameras):
    errored_ids = set(errored_cameras)
    removed_count = 0

    # We use a slice [:] to iterate over a copy so we can modify the original list
    for highway_item in camera_json[:]:
        highway = highway_item.get("highway", {})
        cameras = highway.get("cameras", [])

        original_count = len(cameras)
        highway["cameras"] = [c for c in cameras if c["camera_id"] not in errored_ids]

        diff = original_count - len(highway["cameras"])
        if diff > 0:
            removed_count += diff
            print(f"Removed {diff} cameras from {highway.get('name', 'Unknown')}")

        # Remove the whole highway object if no cameras are left
        if not highway["cameras"]:
            camera_json.remove(highway_item)
            print(f"Removed empty highway: {highway.get('name', 'Unknown')}")

    print(SEP)
    print(f"Total removed: {removed_count}.")
    return camera_json


async def main(
    camera_json,
    rate_limit=DEFAULT_RATE_LIMIT,
    download=True,
    save_file=False,
    output_dir=JSON_OUTPUT_DIR,
    image_dir=IMAGE_DIR,
):
    # Get camera data from json output
    source, camera_ids = get_camera_data(camera_json)

    if download and not image_dir.exists():
        image_dir.mkdir(parents=True, exist_ok=True)

    # Set up aiohttp client
    rate_limiter = asyncio.Semaphore(rate_limit)
    headers, timeout, connector = get_http_settings(rate_limit=rate_limit)

    # Run the checks
    async with aiohttp.ClientSession(
        headers=headers, connector=connector, timeout=timeout
    ) as session:
        tasks = [
            check_camera_async(
                session, source, cam_id, cam_type, rate_limiter, download, IMAGE_DIR
            )
            for cam_id, cam_type in camera_ids
        ]
        results = await tqdm.gather(*tasks, desc="Checking cameras", unit="cam")

    # Separate successful and failed cameras
    alive_cameras = [res["id"] for res in results if res["status"]]
    errored_cameras = [res["id"] for res in results if not res["status"]]
    if download:
        print("Verifying sample images...")
        probably_offline_cams = diff_hash.folder_hash(image_dir)
        print(f"{len(probably_offline_cams)} cameras are probably offline.")
        errored_cameras.extend(probably_offline_cams)
        alive_cameras = list(set(alive_cameras) - set(probably_offline_cams))

    if errored_cameras:
        print(SEP)
        print("Filtering offline cameras")
        print(SEP)
        online_cams = remove_offline_cameras(camera_json, errored_cameras)
        camera_json = online_cams

    alive_percent = len(alive_cameras) / len(camera_ids) * 100
    print(
        f"{len(alive_cameras)}/{len(camera_ids)} ({alive_percent:.2f}%) cameras are online."
    )

    if save_file:
        filename = f"cameras_{source.lower()}_online.json"
        save_path = Path.joinpath(output_dir, filename)
        save_json(camera_json, save_path)
        print(SEP)
        print(f"Saved alive cameras json file to: {filename}")

    return camera_json


if __name__ == "__main__":
    camera_file = load_json("data/spain_original.json")
    winloop.run(main(camera_json=camera_file))
