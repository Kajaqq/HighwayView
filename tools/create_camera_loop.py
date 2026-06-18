import math
from collections.abc import Callable
from pathlib import Path
from typing import Any

from config import CONSTANTS
from tools.utils import get_country, load_json

# ==========================================
# MASTER HIGHWAY SORTING LISTS
# ==========================================
UK_NORTH_SOUTH: list[str] = CONSTANTS.UK.HighwaySort.NORTH_SOUTH
UK_EAST_WEST: list[str] = CONSTANTS.UK.HighwaySort.EAST_WEST
UK_RINGS: list[str] = CONSTANTS.UK.HighwaySort.RINGS

ES_NORTH_SOUTH: list[str] = CONSTANTS.SPAIN.HighwaySort.NORTH_SOUTH
ES_EAST_WEST: list[str] = CONSTANTS.SPAIN.HighwaySort.EAST_WEST
ES_RINGS: list[str] = CONSTANTS.SPAIN.HighwaySort.RINGS

FR_NORTH_SOUTH: list[str] = CONSTANTS.FRANCE.HighwaySort.NORTH_SOUTH
FR_EAST_WEST: list[str] = CONSTANTS.FRANCE.HighwaySort.EAST_WEST
FR_RINGS: list[str] = CONSTANTS.FRANCE.HighwaySort.RINGS

IT_NORTH_SOUTH: list[str] = CONSTANTS.ITALY.HighwaySort.NORTH_SOUTH
IT_EAST_WEST: list[str] = CONSTANTS.ITALY.HighwaySort.EAST_WEST
IT_RINGS: list[str] = CONSTANTS.ITALY.HighwaySort.RINGS

# Map camera regions to countries
COUNTRY_SORT_MAP: dict[str, tuple[list[str], list[str], list[str]]] = {
    "UK": (UK_NORTH_SOUTH, UK_EAST_WEST, UK_RINGS),
    "ES": (ES_NORTH_SOUTH, ES_EAST_WEST, ES_RINGS),
    "FR": (FR_NORTH_SOUTH, FR_EAST_WEST, FR_RINGS),
    "IT": (IT_NORTH_SOUTH, IT_EAST_WEST, IT_RINGS),
}

# ==========================================
# LOOP CONSTANTS
# ==========================================
SEP = CONSTANTS.COMMON.SEPARATOR
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL
COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
DATA_DIR: Path = CONSTANTS.COMMON.DATA_DIR

# A ~10 minute(~90 cameras) loop of the most important highways of the country
HIGHWAY_SEQUENCES: dict[str, list[tuple[str, int]]] = {
    "Spain": CONSTANTS.SPAIN.HIGHWAY_SEQUENCE,
    "France": CONSTANTS.FRANCE.HIGHWAY_SEQUENCE,
    "Italy": CONSTANTS.ITALY.HIGHWAY_SEQUENCE,
    "UK": CONSTANTS.UK.HIGHWAY_SEQUENCE,
}


def get_ring_cameras_angle(cameras: list[dict[str, Any]]) -> Callable[[dict[str, Any]], float]:
    """
    Creates a sorting key function to sort cameras clockwise around their geographic center safely.

    Args:
        cameras (list[dict[str, Any]]): A list of camera dictionaries on the ring highway.

    Returns:
        Callable[[dict[str, Any]], float]: A function mapping a camera to its clockwise angle or km point.
    """
    # Find the center using cameras that have coordinates
    valid_coords = [
        c
        for c in cameras
        if c.get("coords")
        and c["coords"].get("X") is not None
        and c["coords"].get("Y") is not None
    ]

    # If no Coordinates, fallback to sorting by km_point
    if not valid_coords:
        return lambda cam: float(cam.get("camera_km_point", 0) or 0)

    center_x = sum(cam["coords"]["X"] for cam in valid_coords) / len(valid_coords)
    center_y = sum(cam["coords"]["Y"] for cam in valid_coords) / len(valid_coords)

    def get_clockwise_angle(cam: dict[str, Any]) -> float:
        """Helper to get clockwise angle."""
        if (
            not cam.get("coords")
            or cam["coords"].get("X") is None
            or cam["coords"].get("Y") is None
        ):
            return float("inf")

        angle = math.atan2(cam["coords"]["X"] - center_x, cam["coords"]["Y"] - center_y)
        if angle < 0:
            angle += 2 * math.pi
        return angle

    return get_clockwise_angle


def get_sort_order(country_code: str = "UK") -> tuple[list[str], list[str], list[str]]:
    """
    Fetches the highway classification collections for a country.

    Args:
        country_code (str, optional): The country code (e.g., 'UK', 'ES'). Defaults to 'UK'.

    Returns:
        tuple[list[str], list[str], list[str]]: A tuple of lists representing (North-South, East-West, Rings).
    """
    return COUNTRY_SORT_MAP.get(country_code.upper(), ([], [], []))


def sort_cameras(cameras: list[dict[str, Any]], highway: str, country: str) -> list[dict[str, Any]]:
    """
    Sorts a list of cameras based on their highway directionality (NS, EW, or Ring).

    Args:
        cameras (list[dict[str, Any]]): The list of cameras.
        highway (str): The name of the highway.
        country (str): The country code.

    Returns:
        list[dict[str, Any]]: The sorted list of cameras.
    """
    if not cameras:
        return []

    ns, ew, rings = get_sort_order(country)

    def safe_y(cam: dict[str, Any]) -> float:
        """Safely extract Y or fallback to km."""
        if cam.get("coords") and cam["coords"].get("Y") is not None:
            return float(cam["coords"]["Y"])
        km = cam.get("camera_km_point")
        return float(km) if km is not None else float("inf")

    def safe_x(cam: dict[str, Any]) -> float:
        """Safely extract X or fallback to km."""
        if cam.get("coords") and cam["coords"].get("X") is not None:
            return float(cam["coords"]["X"])
        km = cam.get("camera_km_point")
        return float(km) if km is not None else float("inf")

    if highway in ns:
        cameras.sort(key=safe_y)
        return cameras

    elif highway in ew:
        cameras.sort(key=safe_x)
        return cameras

    elif highway in rings:
        cameras.sort(key=get_ring_cameras_angle(cameras))
        return cameras

    else:
        print(
            f"[Info] Highway {highway} not explicitly categorized. Defaulting to Y-sort."
        )
        cameras.sort(key=safe_y)
        return cameras


def sample_cameras(cameras: list[dict[str, Any]], target_count: int, highway_name: str) -> list[dict[str, Any]]:
    """
    Evenly samples a target number of cameras from the provided list.

    Args:
        cameras (list[dict[str, Any]]): The list of available cameras.
        target_count (int): The number of cameras to select.
        highway_name (str): The name of the highway (used for logging).

    Returns:
        list[dict[str, Any]]: The sampled cameras.
    """
    if not cameras or target_count <= 0:
        return []
    cameras_len = len(cameras)
    if target_count >= cameras_len:
        print(
            f"[WARN] Camera allocation for {highway_name} exceeds or equals camera count. Expected: {target_count} Got: {cameras_len}"
        )
        print(f"{highway_name}: Returning all {cameras_len} cameras.")
        return cameras
    print(
        f"{highway_name}: Selecting {target_count} cameras from {cameras_len} available."
    )
    step = cameras_len / target_count
    return [cameras[int(i * step)] for i in range(target_count)]


def process_highway_sequence(cameras: list[dict[str, Any]], sequence_list: list[tuple[str, int]],country: str) -> list[dict[str, Any]]:
    """
    Processes the specified sequence of highways to collect and sort cameras.

    Args:
        cameras (list[dict[str, Any]]): The full parsed dataset grouping highways.
        sequence_list (list[tuple[str, int]]): A list specifying the sequence of highways and the number of
            cameras to sample from each.
        country (str): The country code.

    Returns:
        list[dict[str, Any]]: A compiled playlist containing the appropriately sampled cameras.
    """
    data_map = {item["highway"]["name"]: item["highway"]["cameras"] for item in cameras}

    final_playlist: list[dict[str, Any]] = []

    for highway_name, count in sequence_list:
        real_name = highway_name.split("_")[0]

        if real_name not in data_map:
            print(f"Warning: {real_name} not found in JSON data.")
            continue

        raw_cameras = data_map[real_name]

        valid_cameras: list[dict[str, Any]] = []
        for c in raw_cameras:
            has_coords = (
                bool(c.get("coords"))
                and c["coords"].get("X") is not None
                and c["coords"].get("Y") is not None
            )
            has_km = c.get("camera_km_point") is not None

            if has_coords or has_km:
                valid_cameras.append(c)

        filtered_cameras = valid_cameras

        # --- Italy A04 special case ---
        if real_name == "A04":
            temp_filtered: list[dict[str, Any]] = []
            for c in valid_cameras:
                # Safely extract X and KM values
                x = (
                    c["coords"]["X"]
                    if (c.get("coords") and c["coords"].get("X") is not None)
                    else None
                )
                km = c.get("camera_km_point")

                if "WEST" in highway_name:
                    if (x is not None and x < 9.2) or (
                        x is None and km is not None and km < 125
                    ):
                        temp_filtered.append(c)
                elif "CENTER" in highway_name:
                    if (x is not None and 10.0 < x < 12.0) or (
                        x is None and km is not None and 217 <= km <= 363
                    ):
                        temp_filtered.append(c)
                elif "EAST" in highway_name:
                    if (x is not None and x > 12.0) or (
                        x is None and km is not None and km > 363
                    ):
                        temp_filtered.append(c)

            filtered_cameras = temp_filtered

        if not filtered_cameras:
            print(
                f"  [Warning] Filter for {highway_name} returned 0 cameras. Skipping."
            )
            continue

        sorted_cams = sort_cameras(filtered_cameras, real_name, country)

        sampled = sample_cameras(sorted_cams, count, highway_name)

        final_playlist.extend(sampled)

    return final_playlist


def main(data: str | Path | list[dict[str, Any]],loop_data: list[tuple[str, int]] | None = None) -> list[str]:
    """
    Constructs a looped list of cameras based on configured priorities.

    Args:
        data (str | Path | list[dict[str, Any]]): The input camera data file or parsed data.
        loop_data (list[tuple[str, int]] | None, optional): An explicit loop configuration or None to use defaults.

    Returns:
        list[str]: A list of selected camera IDs representing the slideshow cycle.
    """
    parsed_data = load_json(data)
    country = get_country(parsed_data)
    country_name = COUNTRY_MAP[country]
    print(SEP)
    print(f"Creating loop for {country_name}")
    print(SEP)
    if not loop_data:
        loop_data = HIGHWAY_SEQUENCES[country_name]
    selected_cameras = process_highway_sequence(parsed_data, loop_data, country)
    camera_ids: list[str] = []
    print(SEP)
    print(f"Successfully compiled loop with {len(selected_cameras)} total cameras")
    print(SEP)
    camera_ids.extend(str(cam["camera_id"]) for cam in selected_cameras)
    return camera_ids


if __name__ == "__main__":
    json_file = Path(__file__).parent / "data" / "cameras_uk_online.json"
    uk_loop_sequence = [
        ("M20", 5),
        ("A282", 2),
        ("M25", 8),
    ]
    main(json_file, uk_loop_sequence)
