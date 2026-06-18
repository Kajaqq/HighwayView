import sys
from pathlib import Path

from natsort import natsorted

from config import CONSTANTS
from tools.utils import load_json

COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
SEP = CONSTANTS.COMMON.SEPARATOR


def parse_highways(json_data):
    """
    Counts highway cameras from JSON data.

    Args:
        json_data (list[dict[str, Any]]): The parsed JSON containing highways and cameras.

    Returns:
        list[tuple[str, int]]: A sorted list of tuples, each containing the
            highway name and its total camera count.
    """
    highway_data: list[tuple[str, int]] = []
    for highway_item in json_data:
        highway = highway_item["highway"]
        highway_name: str = highway["name"]
        camera_count: int = len(highway["cameras"])
        highway_data.append((highway_name, camera_count))

    highway_data = natsorted(highway_data)
    return highway_data


def main(highway_data):
    """
    Prints a formatted list of highways with their respective camera counts.

    Args:
        highway_data (list[tuple[str, int]]): The list of highway counts.
    """
    print("\nHighways and camera counts:")
    print(SEP)

    total_cameras = 0
    for highway_name, camera_count in highway_data:
        total_cameras += camera_count
        print(f"{highway_name:15} {camera_count:4} cameras")

    print(SEP)
    print(f"{'Total':15} {total_cameras:4} cameras")


if __name__ == "__main__":
    json_file = sys.argv[1]
    json_data_input = load_json(Path(json_file))
    parsed_data = parse_highways(json_data_input)
    main(parsed_data)
