from pathlib import Path
from typing import Any

from tools.utils import load_json, save_json
from config import CONSTANTS


SEP = CONSTANTS.COMMON.SEPARATOR
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL
COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
DATA_DIR = CONSTANTS.COMMON.DATA_DIR

# A ~10 minute(~90 cameras) loop of the most important highways of the country
HIGHWAY_SEQUENCES = {
    "Spain": CONSTANTS.SPAIN.HIGHWAY_SEQUENCE,
    "France": CONSTANTS.FRANCE.HIGHWAY_SEQUENCE,
    "Italy": CONSTANTS.ITALY.HIGHWAY_SEQUENCE,
}


class NoCamerasProvided(Exception):
    pass


def select_cameras(
    cameras: list[dict], allocation: int
) -> list[Any] | tuple[list[dict], bool] | list[dict]:
    """
    Select cameras to show

    1. Always select the camera with the lowest KM point (start)
    2. Always select the camera with the highest KM point (end)
    3. Divide remaining cameras evenly across the distance
    """

    if not cameras or allocation <= 0:
        raise NoCamerasProvided("No cameras could be read from the provided data.")

    # Sort cameras by kilometer point
    sorted_cameras = sorted(cameras, key=lambda c: c["camera_km_point"])

    if allocation == 1:
        return [sorted_cameras[0]]

    # Get the camera at the start and the camera at the end
    selected = [sorted_cameras[0], sorted_cameras[-1]]

    if allocation == 2:
        return selected

    # Get middle cameras
    pool = sorted_cameras[1:-1]
    start_km = sorted_cameras[0]["camera_km_point"]
    total_distance = sorted_cameras[-1]["camera_km_point"] - start_km
    interval = total_distance / (allocation - 1)

    for i in range(1, allocation - 1):
        if not pool:  # No more cameras to select
            break

        target_km = start_km + (i * interval)  # Calculate the target KM point
        # Find closest camera to target_km
        closest_camera = min(pool, key=lambda c: abs(c["camera_km_point"] - target_km))

        selected.append(closest_camera)
        pool.remove(closest_camera)

    return sorted(selected, key=lambda c: c["camera_km_point"])


def filter_cameras(
    input_data: Path | str | list | dict,
    highway_seq: list[tuple] | None = None,
    save_loop: bool = True,
) -> list[dict]:

    data = load_json(input_data)

    country = data[0]["highway"]["country"]
    country_name = COUNTRY_MAP[country]
    print(SEP)
    print(f"Creating loop for {country_name}...")

    if highway_seq is None:
        highway_seq = HIGHWAY_SEQUENCES[country_name]

    # Create a lookup dict for highway data
    highway_lookup = {h["highway"]["name"]: h for h in data}

    filtered_data = []
    total_cameras_selected = 0

    for highway_name, allocation in highway_seq:
        if highway_data := highway_lookup.get(highway_name):
            cameras = highway_data["highway"]["cameras"]

            cameras_len = len(cameras)

            if cameras_len <= allocation:
                print(
                    f"[WARN] Camera allocation for {highway_name} exceeds or equals camera count. Expected: {allocation} Got: {cameras_len}"
                )
                selected_cameras = cameras
            else:
                selected_cameras = select_cameras(cameras, allocation)

            filtered_data.append(
                {
                    "highway": {
                        "name": highway_name,
                        "country": highway_data["highway"]["country"],
                        "cameras": selected_cameras,
                    }
                }
            )

            count = len(selected_cameras)
            total_cameras_selected += count
            if cameras_len > count:
                print(f"[OK] {highway_name}: {count}/{allocation} cameras selected")
            else:
                print(f"[WARN] {highway_name}: all cameras selected")
        else:
            print(f"[ERROR] {highway_name}: Not found in source data")

    if save_loop:
        output_file = DATA_DIR / f"{country_name}_loop.json"
        save_json(filtered_data, output_file)
        print(SEP)
        print(f"Output written to: {output_file}")

    print(SEP)
    print(f"Total cameras selected: {total_cameras_selected}")
    running_time = (total_cameras_selected * DEFAULT_INTERVAL) / 60
    print(f"Total running time: {running_time:.2f} minutes")
    print(SEP)

    return filtered_data


if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent / "data"
    filter_cameras(base_dir / "cameras_es_online.json", save_loop=True)
