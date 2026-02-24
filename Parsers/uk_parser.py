import asyncio
from collections import defaultdict

from tools.uk_downloader import get_uk_data
from tools.utils import save_json, load_json

async def parse_camera_data(raw_data=None):
    if not raw_data:
        raw_data = await get_uk_data()
    camera_data = load_json(raw_data)
    grouped_highways = defaultdict(list)
    for cam in camera_data:
        cam_desc = cam.get("description").split(' ')
        highway_name = cam_desc[0]
        camera_id = cam_desc[1]
        grouped_highways[highway_name].append(
            {
                "camera_id": camera_id,
                "camera_km_point": 0.0,
                "camera_view": "*",
                "camera_type": "img",
                "coords": {"X": cam.get("longitude"), "Y": cam.get("latitude")},
            }
        )

    final_output = [
        {"highway": {"name": name, "country": "UK", "cameras": cameras}}
        for name, cameras in grouped_highways.items()
    ]

    print(f"Successfully parsed {len(camera_data)} cameras.")
    print(f"Data grouped by {len(final_output)} highways.")
    return final_output



async def get_parsed_data(output_file=None, output_folder=None):
    camera_data = await get_uk_data()
    uk_data = await parse_camera_data(camera_data)
    if output_file:
        save_json(uk_data, output_file)
    elif output_folder:
        save_json(uk_data, output_folder / "cameras_uk_gov.json")
    return uk_data

if __name__ == "__main__":
    asyncio.run(get_parsed_data())
