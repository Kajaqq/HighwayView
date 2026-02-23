import asyncio
import re
from collections import defaultdict
from pathlib import Path

from tools.utils import save_json, load_json
from config import CONSTANTS
from tools.italy_downloader import get_italy_data

CAMERA_BASE_URL = CONSTANTS.ITALY.CAMERA_URL


def parse_autostrade_cameras(raw_data):
    if not raw_data:
        return []

    try:
        data = load_json(raw_data)
    except Exception as e:
        print(f"Error parsing Autostrade JSON: {e}")
        return []

    grouped_highways = defaultdict(list)
    webcams = data.get("webcams", [])

    for cam in webcams:
        highway_name = cam.get("c_str", "Unknown")
        if highway_name == "A4":
            highway_name = "A04"

        video_fragment = cam.get("frames", {}).get("V", {}).get("t_url", "")
        if not video_fragment:
            continue

        full_url = f"{CAMERA_BASE_URL}{video_fragment}"

        km_ini = cam.get("n_prg_km_ini")
        km_fin = cam.get("n_prg_km_fin")

        if km_ini is not None and km_fin is not None:
            if km_ini < km_fin:
                direction = "+"
            elif km_ini > km_fin:
                direction = "-"
            else:
                direction = "*"
        else:
            direction = "*"

        camera_entry = {
            "camera_id": str(cam.get("c_tel")),
            "camera_km_point": cam.get("n_prg_km", 0.0),
            "camera_view": direction,
            "camera_type": "vid",
            "url": full_url,
            "coords": {
                "X": cam.get("n_crd_lon"),
                "Y": cam.get("n_crd_lat"),
            },
        }
        grouped_highways[highway_name].append(camera_entry)

    return [
        {
            "highway": {
                "name": name,
                "country": "IT",
                "cameras": cameras,
            }
        }
        for name, cameras in grouped_highways.items()
    ]


def parse_a22_cameras(raw_data):
    if not raw_data:
        return []

    try:
        data = load_json(raw_data)
    except Exception as e:
        print(f"Error parsing A22 JSON: {e}")
        return []

    cameras = []
    for region in data.values():
        for cam in region:
            desc = cam.get("Descrizione", "").lower()
            if "modena" in desc:
                direction = "+"
            elif "brennero" in desc:
                direction = "-"
            else:
                direction = "*"

            img_url = cam.get("Immagine", "")
            if img_url.startswith("//"):
                img_url = "https:" + img_url

            camera_entry = {
                "camera_id": f"{cam.get('ID')}",
                "camera_km_point": float(cam.get("Distanza", 0)),
                "camera_view": direction,
                "camera_type": "img",
                "url": img_url,
                "coords": {
                    "X": cam.get("Lng"),
                    "Y": cam.get("Lat"),
                },
            }
            cameras.append(camera_entry)

    return [
        {
            "highway": {
                "name": "A22",
                "country": "IT",
                "cameras": cameras,
            }
        }
    ]


def parse_a4_abp(raw_data):
    BASE_URL = CONSTANTS.ITALY.A4.ABP.BASE_ABP_URL
    if not raw_data:
        return []
    try:
        data = load_json(raw_data)
        cameras = []
        for cam in data:
            cam_id = str(cam.get("id", ""))
            km_start = cam["name"].find("km")
            km_from_name = cam["name"][km_start + 2 : km_start + 6].strip()
            km_point = float(km_from_name) if km_from_name.isdigit() else 0.0
            video_url = cam.get("url", "")
            video_url = BASE_URL + video_url

            if not video_url:
                continue

            camera_entry = {
                "camera_id": cam_id,
                "camera_km_point": km_point,
                "camera_view": "*",
                "camera_type": "vid"
                if video_url.endswith((".mp4", ".m3u8"))
                else "img",
                "url": video_url,
                "coords": {
                    "X": cam.get("lng"),
                    "Y": cam.get("lat"),
                },
            }
            cameras.append(camera_entry)
        return cameras  # noqa: TRY300
    except Exception as e:
        print(f"Error parsing A4 ABP data: {e}")
        return []


def parse_a4_cav(raw_data):
    if not raw_data:
        return []
    base_ip_url = CONSTANTS.ITALY.A4.CAV.WEBCAM_URL
    try:
        json_file = load_json(raw_data)
        file_features = json_file.get("features", [])
        cameras = []

        for feature in file_features:
            camera_data = feature.get("properties", {})
            cam_url = camera_data.get("URL", "")
            if cam_url.startswith("https://inviaggio.autobspd.it/"):
                # Duplicated cameras from ABP
                continue
            if camera_data.get("VIS_WEB") == "S":
                cam_id = str(camera_data.get("IDTELECAMERA", ""))
                prog_km = float(camera_data.get("PROG_KM", 0.0))

                if cam_url == "---":
                    cam_url = base_ip_url.format(ip=camera_data.get("IP", ""))

                geometry = feature.get("geometry", {})
                coords = geometry.get("coordinates", [None, None])

                camera_entry = {
                    "camera_id": cam_id,
                    "camera_km_point": prog_km,
                    "camera_view": "*",
                    "camera_type": "img",
                    "url": cam_url,
                    "coords": {
                        "X": coords[0],
                        "Y": coords[1],
                    },
                }
                cameras.append(camera_entry)
        return cameras  # noqa: TRY300
    except Exception as e:
        print(f"Error parsing A4 CAV data: {e}")
        return []


def parse_a4_satap(raw_data):
    if not raw_data:
        return []
    keyword_start = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[0]
    keyword_end = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[1]

    try:
        blocks = re.findall(
            f"{re.escape(keyword_start)}(.*?){re.escape(keyword_end)}",
            raw_data,
            re.DOTALL,
        )

        cameras = []
        for block in blocks:
            title_match = re.search(r"<h2>(.*?)</h2>", block)
            title = title_match.group(1).strip() if title_match else "Unknown"

            km_point = 0.0
            km_match = re.search(r"KM\s*(\d+)\+(\d+)", title)
            if km_match:
                km_point = float(km_match.group(1)) + float(km_match.group(2)) / 1000

            video_match = re.search(r'href="(https?://[^"]+\.mp4)"', block)
            video_url = video_match.group(1) if video_match else None
            cam_type = "vid"

            if not video_url:
                continue

            cam_id = video_url.split("/")[-1].split(".")[0]

            camera_entry = {
                "camera_id": f"{cam_id}",
                "camera_km_point": km_point,
                "camera_view": "*",
                "camera_type": cam_type,
                "url": video_url,
                "coords": {
                    "X": None,
                    "Y": None,
                },
            }
            cameras.append(camera_entry)
        return cameras  # noqa: TRY300
    except Exception as e:
        print(f"Error parsing A4 SATAP data: {e}")
        return []


async def get_parsed_data(output_file=None, output_folder=None):
    # 1. Download all raw data
    raw_data = await get_italy_data()

    # 2. Parse Autostrade (Main provider)
    parsed_data = parse_autostrade_cameras(raw_data["autostrade"])

    # 3. Parse A22 Brennero
    a22_parsed = parse_a22_cameras(raw_data["a22"])
    if a22_parsed:
        parsed_data.extend(a22_parsed)

    # 4. Parse and Merge A4 Providers
    a4_cameras = []
    a4_cameras.extend(parse_a4_abp(raw_data["a4_abp"]))
    a4_cameras.extend(parse_a4_cav(raw_data["a4_cav"]))
    a4_cameras.extend(parse_a4_satap(raw_data["a4_satap"]))

    if a4_cameras:
        # Check if A4 already exists in parsed_data (from Autostrade)
        a4_entry = next((h for h in parsed_data if h["highway"]["name"] == "A04"), None)
        if a4_entry:
            a4_entry["highway"]["cameras"].extend(a4_cameras)
        else:
            parsed_data.append(
                {
                    "highway": {
                        "name": "A04",
                        "country": "IT",
                        "cameras": a4_cameras,
                    }
                }
            )

    # Remove duplicates and sort cameras by KM point for each highway
    for entry in parsed_data:
        unique_cameras = []
        seen_urls = set()
        seen_ids = {}  # Map ID to camera object

        for cam in entry["highway"]["cameras"]:
            url = cam["url"]
            cam_id = cam["camera_id"]

            # 1. Check URL duplication
            if url in seen_urls:
                continue

            # 2. Check ID duplication
            if cam_id in seen_ids:
                existing_cam = seen_ids[cam_id]

                # Check coordinates match (with some tolerance)
                coords1 = cam["coords"]
                coords2 = existing_cam["coords"]

                match = False
                if (
                    coords1["X"] is not None
                    and coords1["Y"] is not None
                    and coords2["X"] is not None
                    and coords2["Y"] is not None
                ):
                    if (
                        abs(coords1["X"] - coords2["X"]) < 0.0001
                        and abs(coords1["Y"] - coords2["Y"]) < 0.0001
                    ):
                        match = True
                elif coords1["X"] is None and coords2["X"] is None:
                    # If both have no coords, assume match if IDs match (risky but standard fallback)
                    match = True

                if match:
                    # Duplicate ID and matching coords -> skip this one
                    continue
                else:
                    # Duplicate ID but different coords -> rename ID
                    cam["camera_id"] = f"{cam_id}_dup"

            # If we get here, it's a valid camera to keep
            seen_urls.add(url)
            seen_ids[cam["camera_id"]] = cam  # Use the potentially renamed ID
            unique_cameras.append(cam)

        # Replace with filtered list and sort
        entry["highway"]["cameras"] = sorted(
            unique_cameras, key=lambda x: x["camera_km_point"]
        )

    if output_file:
        save_json(parsed_data, output_file)
    elif output_folder:
        save_json(parsed_data, output_folder / "cameras_it.json")

    total_cameras = sum(len(h["highway"]["cameras"]) for h in parsed_data)
    print(f"Successfully parsed {total_cameras} cameras.")
    print(f"Data grouped by {len(parsed_data)} highways.")
    return parsed_data


if __name__ == "__main__":
    output = Path(__file__).parent.parent / "data" / "cameras_it.json"
    asyncio.run(get_parsed_data(output))
