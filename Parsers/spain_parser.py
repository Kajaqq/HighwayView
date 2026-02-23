import json
from base64 import b64decode
from collections import defaultdict
from pathlib import Path

from tools.utils import download_post, save_json, xor_decode
from config import CONSTANTS

DATA_URL = CONSTANTS.SPAIN.CAMERA_API
XOR_KEY = CONSTANTS.SPAIN.XOR_KEY


async def get_camera_data():
    download_link = DATA_URL
    xored_data = await download_post(download_link)
    decoded_data = decode_data(xored_data)
    return decoded_data


def decode_data(camaras_data):
    try:
        decoded_bytes = b64decode(camaras_data, validate=True)
    except Exception as exc:
        raise ValueError(f"Base64 decode failed: {exc}") from exc

    json_text = xor_decode(decoded_bytes, XOR_KEY)

    print("Successfully downloaded camera data.")
    return json_text


async def parse_camera_data(json_data):
    try:
        raw_data = json.loads(json_data)
    except json.JSONDecodeError:
        print("Error: Failed to decode the input JSON file.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during JSON parsing: {e}")
        return None

    try:
        grouped_highways = defaultdict(list)
        camaras = raw_data.get("camaras") or []
        for cam in camaras:
            highway_name = cam.get("carretera") or "Unknown"
            grouped_highways[highway_name].append(
                {
                    "camera_id": cam.get("idCamara"),
                    "camera_km_point": cam.get("pk"),
                    "camera_view": cam.get("sentido"),
                    "camera_type": "img",
                    "coords": {"X": cam.get("coordX"), "Y": cam.get("coordY")},
                }
            )

        final_output = [
            {"highway": {"name": name, "country": "ES", "cameras": cameras}}
            for name, cameras in grouped_highways.items()
        ]

        print(f"Successfully parsed {len(camaras)} cameras.")
        print(f"Data grouped by {len(final_output)} highways.")

        if final_output:
            return final_output

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


async def get_parsed_data(output_file=None, output_folder=None):
    camera_data = await get_camera_data()
    spain_data = await parse_camera_data(camera_data)
    if output_file:
        save_json(spain_data, output_file)
    elif output_folder:
        save_json(spain_data, output_folder / "cameras_es_gov.json")
    return spain_data


if __name__ == "__main__":
    import asyncio

    OUTPUT_DIR = Path("../data/cameras_es_gov.json")
    data = asyncio.run(get_camera_data())
    asyncio.run(parse_camera_data(data))
