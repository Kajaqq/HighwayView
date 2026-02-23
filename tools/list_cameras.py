import sys

from natsort import natsorted

from config import CONSTANTS
from tools.utils import load_json


COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
SEPARATOR = CONSTANTS.COMMON.SEPARATOR


def parse_highways(json_data: list[dict]) -> list[tuple[str, int, str]]:
    highway_data = []
    for highway_item in json_data:
        highway = highway_item["highway"]
        highway_name = highway["name"]
        camera_count = len(highway["cameras"])
        highway_data.append((highway_name, camera_count))

    highway_data = natsorted(highway_data)
    return highway_data


def main(highway_data: list[tuple[str, int, str]]):
    print("\nHighways and camera counts:")
    print(SEPARATOR)

    total_cameras = 0
    for highway_name, camera_count in highway_data:
        total_cameras += camera_count
        print(f"{highway_name:15} {camera_count:4} cameras")

    print(SEPARATOR)
    print(f"{'Total':15} {total_cameras:4} cameras")


if __name__ == "__main__":
    json_file = sys.argv[1]
    json_data = load_json(json_file)
    parsed_data = parse_highways(json_data)
    main(parsed_data)
