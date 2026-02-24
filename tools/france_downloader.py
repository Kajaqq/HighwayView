import asyncio
import winloop
import json
import aiohttp
import aiofiles

from pathlib import Path
from tools.france_asfa_deobfuscate import get_complete_url as get_asfa_url
from config import CONSTANTS
from tools.utils import (
    download,
    unix_to_datetime,
    get_http_settings,
    save_json_async,
)


async def save_asfa_file(asfa_data, output_file):
    output_file = Path(output_file)
    output_path = output_file.parent
    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(output_file, "w", encoding="utf-8") as f:
        await f.write(asfa_data)


async def get_gov_url(session):
    base_url = CONSTANTS.FRANCE.BASE_URL
    timestamp_url = f"{base_url}{CONSTANTS.FRANCE.TIMESTAMP_URL}"
    camera_url = f"{base_url}{CONSTANTS.FRANCE.CAMERA_API}"

    try:
        timestamp_raw = await download(url=timestamp_url, session=session)
        timestamp = json.loads(timestamp_raw)[0]

        # Ensure we have an integer before formatting
        if isinstance(timestamp, str):
            timestamp = int(timestamp)

        timestamp_formatted = unix_to_datetime(timestamp)
        return camera_url.format(datetime=timestamp_formatted)
    except (ValueError, IndexError, Exception) as e:
        print(f"Error fetching/parsing timestamp: {e}")
        return None


async def download_asfa(session):
    asfa_camera_url = await get_asfa_url()
    return await download(url=asfa_camera_url, session=session)


async def download_gov(session):
    gov_camera_url = await get_gov_url(session=session)
    if not gov_camera_url:
        return None
    return await download(url=gov_camera_url, session=session)


async def get_france_data(
    asfa_only=False, gov_only=False, output_file_gov=None, output_file_asfa=None
):
    headers, timeout, connector = get_http_settings()
    asfa_camera_data = None
    gov_camera_data = None
    async with aiohttp.ClientSession(
        headers=headers, connector=connector, timeout=timeout
    ) as session:
        # Determine what needs to be downloaded
        fetch_asfa = asfa_only or (not gov_only)
        fetch_gov = gov_only or (not asfa_only)

        # Create tasks for downloads
        asfa_task = (
            download_asfa(session) if fetch_asfa else asyncio.sleep(0, result=None)
        )
        gov_task = download_gov(session) if fetch_gov else asyncio.sleep(0, result=None)

        # Execute downloads concurrently
        asfa_camera_data, gov_camera_data = await asyncio.gather(asfa_task, gov_task)

    # Save results if paths provided
    save_tasks = []
    if asfa_camera_data and output_file_asfa:
        save_tasks.append(save_asfa_file(asfa_camera_data, output_file_asfa))
    if gov_camera_data and output_file_gov:
        save_tasks.append(save_json_async(gov_camera_data, output_file_gov))

    if save_tasks:
        await asyncio.gather(*save_tasks)

    return asfa_camera_data, gov_camera_data


if __name__ == "__main__":
    # Use paths relative to project root or absolute paths
    data_dir = Path(__file__).parent.parent / "data"
    out_asfa = data_dir / "cameras_fr_asfa.js"
    out_gov = data_dir / "cameras_fr_gov.json"

    winloop.run(get_france_data(output_file_gov=out_gov, output_file_asfa=out_asfa))
