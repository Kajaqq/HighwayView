import asyncio
from pathlib import Path
from typing import Any

import aiohttp
import winloop
from tqdm.asyncio import tqdm

from tools.utils import load_json, create_url, save_json, get_country
import tools.diff_hash as diff_hash
from Downloaders.base_downloader import GenericDownloader, HTTPError
from config import CONSTANTS

SEP: str = CONSTANTS.COMMON.SEPARATOR
DEFAULT_RATE_LIMIT: int = CONSTANTS.COMMON.RATE_LIMIT
JSON_OUTPUT_DIR: Path = CONSTANTS.COMMON.DATA_DIR
IMAGE_DIR: Path = CONSTANTS.COMMON.IMG_DIR


async def save_image(
    camera_id: str | int, ext: str, img_bytes: bytes, output_dir: Path = IMAGE_DIR
) -> None:
    """
    Saves an image byte sequence to a file.

    Args:
        camera_id (str | int): The camera identifier, used for the filename.
        ext (str): The file extension (e.g., '.jpg').
        img_bytes (bytes): The raw image byte data.
        output_dir (Path, optional): The directory to save the image. Defaults to IMAGE_DIR.
    """
    filename = f"{camera_id}{ext}"
    file_path = output_dir / filename
    await asyncio.to_thread(file_path.write_bytes, img_bytes)


def get_camera_data(json_data: list[dict[str, Any]]) -> list[Any]:
    """
    Extracts the country and a list of camera target strings.

    Args:
        json_data (list[dict[str, Any]]): The parsed camera data.

    Returns:
        list[Any]: A list containing the country code and a list of [camera_id, camera_url/type] pairs.
    """
    country = get_country(json_data)
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


async def check_camera(
    client: aiohttp.ClientSession,
    source: str,
    camera_id: str | int,
    camera_type: str,
    rate_limiter: asyncio.Semaphore,
    download: bool,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Checks the status of a single camera and optionally downloads its latest image/video.

    Args:
        client (aiohttp.ClientSession): The HTTP client session.
        source (str): The country code.
        camera_id (str | int): The camera identifier.
        camera_type (str): The camera type or URL.
        rate_limiter (asyncio.Semaphore): Concurrency limit semaphore.
        download (bool): Whether to download the media to disk.
        output_dir (Path | None, optional): Directory to save downloaded media. Defaults to None.

    Returns:
        dict[str, Any]: A dictionary containing the camera 'id' and 'status' (or False if failed).
    """

    def _validate_response(bytes_: bytes) -> None:
        if len(bytes_) < 1000:
            raise HTTPError(f"Response too small: {len(bytes_)} bytes")

    if source != "IT":
        url, ext = create_url(source, camera_id, camera_type)
    else:
        # Special case for Italy where urls are in the data directly
        url = camera_type
        ext = CONSTANTS.ITALY.VIDEO_EXT

    async with rate_limiter:
        response_bytes = b""
        try:
            async with client.get(url, allow_redirects=True) as response:
                response.raise_for_status()
                response_bytes = await response.read()
                status_code = response.status
                _validate_response(response_bytes)

            if not download:
                return {"id": camera_id, "status": status_code}
            else:
                if output_dir:
                    await save_image(camera_id, ext or "", response_bytes, output_dir)
                return {"id": camera_id, "status": status_code}
        except TimeoutError, HTTPError, aiohttp.ClientError, aiohttp.ClientPayloadError:
            return {"id": camera_id, "status": False, "len": len(response_bytes)}


def remove_offline_cameras(
    camera_json: list[dict[str, Any]], errored_cameras: list[str | int]
) -> list[dict[str, Any]]:
    """
    Removes offline cameras from the JSON dataset.

    Args:
        camera_json (list[dict[str, Any]]): The full camera dataset.
        errored_cameras (list[str | int]): A list of camera IDs that failed verification.

    Returns:
        list[dict[str, Any]]: The filtered dataset containing only online cameras.
    """
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
    camera_json: list[dict[str, Any]],
    rate_limit: int = DEFAULT_RATE_LIMIT,
    download: bool = True,
    save_file: bool = False,
    output_dir: Path = JSON_OUTPUT_DIR,
    image_dir: Path = IMAGE_DIR,
) -> list[dict[str, Any]]:
    """
    Main orchestration routine to verify all cameras in a JSON dataset,
    remove offline ones, and generate a cleaned list.

    Args:
        camera_json (list[dict[str, Any]]): The input camera data.
        rate_limit (int, optional): The concurrency limit for checking. Defaults to DEFAULT_RATE_LIMIT.
        download (bool, optional): Whether to download images to check for visual duplication. Defaults to True.
        save_file (bool, optional): Whether to save the verified JSON data to disk. Defaults to False.
        output_dir (Path, optional): Directory to save the verified JSON. Defaults to JSON_OUTPUT_DIR.
        image_dir (Path, optional): Directory to temporarily save verification images. Defaults to IMAGE_DIR.

    Returns:
        list[dict[str, Any]]: The cleaned list of verified cameras.
    """
    # Get camera data from json output
    source, camera_ids = get_camera_data(camera_json)

    if download:
        has_dir = await asyncio.to_thread(image_dir.exists)
        if not has_dir:
            await asyncio.to_thread(image_dir.mkdir, parents=True, exist_ok=True)

    # Set up aiohttp client
    rate_limiter = asyncio.Semaphore(rate_limit)
    downloader = GenericDownloader(
        timeout_int=CONSTANTS.COMMON.HTTP_TIMEOUT, rate_limit=rate_limit
    )
    headers, timeout, connector = await downloader.get_settings()

    # Run the checks
    async with aiohttp.ClientSession(
        headers=headers, connector=connector, timeout=timeout
    ) as session:
        tasks = [
            check_camera(
                session, source, cam_id, cam_type, rate_limiter, download, image_dir
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
        if probably_offline_cams:
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
        filename = f"cameras_{str(source).lower()}_online.json"
        save_path = Path.joinpath(output_dir, filename)
        save_json(camera_json, save_path)
        print(SEP)
        print(f"Saved alive cameras json file to: {filename}")

    return camera_json


if __name__ == "__main__":
    camera_file = load_json("data/spain_original.json")
    winloop.run(main(camera_json=camera_file))
