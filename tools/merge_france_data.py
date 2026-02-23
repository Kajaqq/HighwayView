import copy
from collections.abc import Iterable
from pathlib import Path

from tools.utils import haversine_km, save_json, load_json


def flatten_cameras(data: Iterable[dict]) -> list[dict]:
    """Flatten highways -> cameras into a simple list for spatial comparison."""
    return [
        {
            "id": cam.get("camera_id"),
            "lat": cam.get("coords", {}).get("Y"),
            "lon": cam.get("coords", {}).get("X"),
        }
        for entry in data
        for cam in entry.get("highway", {}).get("cameras", [])
    ]


def find_asfa_duplicates(
    gov_cams: list[dict], asfa_cams: list[dict], threshold_km: float = 0.1
) -> set[str]:
    """Identify possible ASFA camera duplicates."""
    dupes = set()
    for cam in asfa_cams:
        lat1, lon1 = cam["lat"], cam["lon"]
        if lat1 is None or lon1 is None:
            continue

        if any(
            (lat2 := gov_cam["lat"]) is not None
            and (lon2 := gov_cam["lon"]) is not None
            and (haversine_km(lat1, lon1, lat2, lon2) <= threshold_km)
            for gov_cam in gov_cams
        ):
            dupes.add(cam["id"])
    return dupes


def filter_asfa_data(asfa_data: list[dict], dupes: set[str]) -> list[dict]:
    """Remove duplicate cameras and drop highways that become empty."""
    filtered = []
    for highway_item in asfa_data:
        highway = highway_item.get("highway", {})
        remaining = [
            c for c in highway.get("cameras", []) if c.get("camera_id") not in dupes
        ]
        if remaining:
            filtered.append({"highway": {**highway, "cameras": remaining}})
    return filtered


def merge_highways(gov_data: list[dict], asfa_data: list[dict]) -> list[dict]:
    """
    Consolidate ASFA cameras into government highways or add new highways.
    Returns a new list sorted by highway name.
    """

    merged_map = {
        entry["highway"]["name"]: copy.deepcopy(entry)
        for entry in gov_data
        if entry.get("highway", {}).get("name")
    }

    for entry in asfa_data:
        asfa_highway = entry.get("highway", {})
        name = asfa_highway.get("name")

        if name in merged_map:
            target_cameras = merged_map[name]["highway"].setdefault("cameras", [])
            existing_ids = {c.get("camera_id") for c in target_cameras}

            target_cameras.extend(
                cam
                for cam in asfa_highway.get("cameras", [])
                if cam.get("camera_id") not in existing_ids
            )
        else:
            merged_map[name] = copy.deepcopy(entry)

    return [merged_map[name] for name in sorted(merged_map)]


def merge_france_data(
    gov_data: list[dict], asfa_data: list[dict], verbose: bool = True
) -> list[dict]:
    """Main entry point for in-memory merging of France camera data."""
    if not gov_data:
        return asfa_data or []
    if not asfa_data:
        return gov_data

    gov_flat = flatten_cameras(gov_data)
    asfa_flat = flatten_cameras(asfa_data)

    dupes = find_asfa_duplicates(gov_flat, asfa_flat, threshold_km=0.20)
    if verbose:
        print(f"Removed {len(dupes)} duplicates.")

    asfa_filtered = filter_asfa_data(asfa_data, dupes)
    return merge_highways(gov_data, asfa_filtered)


if __name__ == "__main__":
    # Example usage for CLI testing
    try:
        gov_data = load_json("../data/cameras_fr_gov.json")
        asfa_data = load_json("../data/cameras_fr_asfa.json")
        merged_file = Path("../data/cameras_fr_merged.json")
        merged = merge_france_data(gov_data, asfa_data)
        save_json(merged, merged_file)
        print(f"Successfully merged and saved to {merged_file}")
    except Exception as e:
        print(f"Error during manual merge: {e}")
