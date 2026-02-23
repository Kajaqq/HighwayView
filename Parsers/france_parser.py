import ast
import json
import re
import asyncio
from collections import defaultdict

from tools.utils import (
    save_json,
    convert_to_wgs84,
)
from tools.france_downloader import get_france_data as download_france_data
from tools.merge_france_data import merge_france_data
from config import CONSTANTS

# Unified regex to handle A, N, RN, D, and M roads
ROAD_REGEX = re.compile(r"\b(A|N|RN|D|M)(\d+)\b", re.IGNORECASE)
PR_REGEX = re.compile(r"PR\s*(\d+)(?:\+(\d+))?", re.IGNORECASE)

UNKNOWN_MAPPING = CONSTANTS.FRANCE.UNKNOWN_MAPPING


def _extract_highway_name(text, camera_id=None):
    """
    Standardizes highway names (e.g., RN 205 -> N-205, A132 -> A-132).
    Uses camera_id as a fallback if the name cannot be extracted from text.
    """
    # 1. Try to extract from text using regex
    if text:
        match = ROAD_REGEX.search(text)
        if match:
            prefix = match.group(1).upper()
            if prefix == "RN":
                prefix = "N"
            return f"{prefix}-{match.group(2)}"

    # 2. Fallback to hardcoded mapping using camera_id
    if camera_id and camera_id in UNKNOWN_MAPPING:
        mapped_name = UNKNOWN_MAPPING[camera_id]
        # Ensure the mapped name is also standardized (e.g., A27 -> A-27)
        match = ROAD_REGEX.search(mapped_name)
        if match:
            prefix = match.group(1).upper()
            if prefix == "RN":
                prefix = "N"
            return f"{prefix}-{match.group(2)}"
        return mapped_name

    return "Unknown"


def _format_highway_output(grouped_highways):
    """Converts the internal grouping dictionary to the final list format."""
    return [
        {"highway": {"name": name, "country": "FR", "cameras": cameras}}
        for name, cameras in sorted(grouped_highways.items())
    ]


def parse_gov_cameras(baguettes):
    try:
        raw_data = json.loads(baguettes)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error decoding Gov JSON: {e}")
        return []

    grouped_highways = defaultdict(list)
    features = raw_data.get("features") or []

    for feature in features:
        props = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        full_label = props.get("libelleCamera") or ""
        camera_id = feature.get("id", "")

        km_point = 0.0
        pr_match = PR_REGEX.search(full_label)
        if pr_match:
            km = int(pr_match.group(1))
            meters = int(pr_match.group(2)) if pr_match.group(2) else 0
            km_point = km + (meters / 1000.0)

        flux_type = props.get("typeFlux") or ""
        cam_type = (
            "vid"
            if flux_type == "VIDEO"
            else "img"
            if flux_type == "IMAGE"
            else "unknown"
        )

        coords_in = geometry.get("coordinates") or []
        lon, lat = (
            convert_to_wgs84(coords_in[0], coords_in[1])
            if len(coords_in) >= 2
            else (0.0, 0.0)
        )

        highway_name = _extract_highway_name(full_label, camera_id)
        grouped_highways[highway_name].append(
            {
                "camera_id": camera_id,
                "camera_km_point": round(km_point, 3),
                "camera_view": "*",
                "camera_type": cam_type,
                "coords": {"X": round(lon, 6), "Y": round(lat, 6)},
            }
        )

    print(f"Successfully parsed {len(features)} government cameras.")
    print(f"Data grouped by {len(grouped_highways)} highways.")
    return _format_highway_output(grouped_highways)


def parse_asfa_cameras(asfa_baguettes):
    try:
        data_string = asfa_baguettes.strip()
        data_string = re.sub(r"^var\s+\w+\s*=\s*", "", data_string)
        data_string = re.sub(r";\s*\w+\.\w+\(.*\);?$", "", data_string)
        parsed_data = ast.literal_eval(data_string)
    except Exception as e:
        print(f"Error parsing ASFA data: {e}")
        return []

    grouped_highways = defaultdict(list)
    for item in parsed_data:
        # Structure: [coords, _, _, description, metadata]
        coords, _, _, description, metadata = item
        camera_id = metadata.get("id")

        highway_name = _extract_highway_name(description, camera_id)
        grouped_highways[highway_name].append(
            {
                "camera_id": camera_id,
                "camera_km_point": 0.0,
                "camera_view": "*",
                "camera_type": "asfa_vid",
                "coords": {
                    "X": coords[1],
                    "Y": coords[0],
                },  # ASFA coords are [lat, lon]
            }
        )

    print(f"Successfully parsed {len(parsed_data)} private cameras.")
    print(f"Data grouped by {len(grouped_highways)} highways.")
    return _format_highway_output(grouped_highways)


def parse_data(
    baguettes=None, asfa_baguettes=None, output_file_gov=None, output_file_asfa=None
):
    gov_cameras = parse_gov_cameras(baguettes) if baguettes else []
    if gov_cameras and output_file_gov:
        save_json(gov_cameras, output_file_gov)

    asfa_cameras = parse_asfa_cameras(asfa_baguettes) if asfa_baguettes else []
    if asfa_cameras and output_file_asfa:
        save_json(asfa_cameras, output_file_asfa)

    return gov_cameras, asfa_cameras


async def get_parsed_data(
    output_file_gov=None,
    output_file_asfa=None,
    output_file_merged=None,
    output_folder=None,
):
    asfa_raw, gov_raw = await download_france_data()
    gov_cameras, asfa_cameras = parse_data(
        gov_raw, asfa_raw, output_file_gov, output_file_asfa
    )

    merged_data = merge_france_data(gov_cameras, asfa_cameras)

    if output_file_merged:
        save_json(merged_data, output_file_merged)

    if output_folder:
        save_raw_data(output_folder, merged_data, asfa_raw, gov_raw)

    return merged_data


def save_raw_data(output_folder, merged_data, asfa_raw, gov_raw):
    output_file_gov = "cameras_fr_gov.json"
    output_file_asfa = "cameras_fr_asfa.json"
    output_file_merged = "cameras_fr_merged.json"

    save_json(asfa_raw, output_folder / output_file_asfa)
    save_json(gov_raw, output_folder / output_file_gov)
    save_json(merged_data, output_folder / output_file_merged)
    print(
        f"France raw data saved to {output_file_gov}, {output_file_asfa}, {output_file_merged}."
    )


if __name__ == "__main__":
    asyncio.run(
        get_parsed_data(
            output_file_gov="data/cameras_fr_gov.json",
            output_file_asfa="data/cameras_fr_asfa.json",
            output_file_merged="data/cameras_fr.json",
        )
    )
