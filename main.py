import winloop
from argparse import Namespace
from pathlib import Path
from typing import Any

from Parsers import france_parser, italy_parser, spain_parser, uk_parser
from tools.camera_check import main as camera_check
from config import CONSTANTS
from tools.create_camera_loop import main as create_loop
from tools.create_html import main as create_html_main

SEP: str = CONSTANTS.COMMON.SEPARATOR
DEFAULT_RATE_LIMIT: int = CONSTANTS.COMMON.RATE_LIMIT
SPAIN_RATE_LIMIT: int = CONSTANTS.SPAIN.RATE_LIMIT
ITALY_RATE_LIMIT: int = CONSTANTS.ITALY.RATE_LIMIT
UK_RATE_LIMIT: int = CONSTANTS.UK.RATE_LIMIT
DEFAULT_INTERVAL: int = CONSTANTS.COMMON.SLIDESHOW_INTERVAL

JSON_OUTPUT_DIR: Path = CONSTANTS.COMMON.DATA_DIR
HTML_OUTPUT_DIR: Path = CONSTANTS.COMMON.HTML_DIR


def create_html_files(
    input_data: list[dict[str, Any]],
    output_dir: Path,
    camera_ids: list[str] | None = None,
    interval: int = DEFAULT_INTERVAL,
) -> None:
    """
    Creates an HTML slideshow from the parsed camera data.

    Args:
        input_data (list[dict[str, Any]]): The parsed camera data.
        output_dir (Path): The directory to save the HTML file.
        camera_ids (list[str] | None, optional): Specific camera IDs to include in the slideshow. Defaults to None.
        interval (int, optional): The slideshow interval in seconds. Defaults to DEFAULT_INTERVAL.
    """
    if interval < 3:
        print(f"Warning: Interval {interval}s is too short. Setting to minimum: 3s")
        interval = 3
    elif interval > 60:
        print(f"Warning: Interval {interval}s is too long. Setting to maximum: 60s")
        interval = 60

    args = Namespace(
        json_file=input_data,
        output_file=None,  # Let create_html.py determine the filename automatically
        output_dir=output_dir,
        camera_ids=camera_ids,
        highways=None,
        interval=interval,
        sort=False,
        include_unknown=False,
    )
    create_html_main(args)


async def get_camera_data(
    country: str, save_raw: bool, save_checked: bool, output_dir: Path
) -> list[dict[str, Any]]:
    """
    Downloads, parses, and explicitly checks cameras for a given country.

    Args:
        country (str): The country name (e.g., 'Spain', 'France', 'Italy', 'UK').
        save_raw (bool): Whether to save the raw JSON data.
        save_checked (bool): Whether to save the checked/online JSON data.
        output_dir (Path): The output directory for the files.

    Raises:
        ValueError: If an invalid country name is provided.

    Returns:
        list[dict[str, Any]]: The parsed list of online cameras for the country.
    """
    print(SEP)
    print(f"Downloading {country} data...")
    print(SEP)
    rate_limit = DEFAULT_RATE_LIMIT

    save_raw_path = output_dir if save_raw else None

    if country == "Spain":
        country_data = await spain_parser.get_parsed_data(save_raw_path)
        rate_limit = SPAIN_RATE_LIMIT

    elif country == "France":
        country_data = await france_parser.get_parsed_data(output_folder=save_raw_path)

    elif country == "Italy":
        country_data = await italy_parser.get_parsed_data(output_folder=save_raw_path)

    elif country == "UK":
        country_data = await uk_parser.get_parsed_data(output_folder=save_raw_path)
        rate_limit = UK_RATE_LIMIT

    else:
        raise ValueError(f"Invalid country: {country}")

    checked_country_data = await camera_check(
        camera_json=country_data,
        rate_limit=rate_limit,
        output_dir=output_dir,
        save_file=save_checked,
    )
    return checked_country_data


async def main() -> None:
    """
    Main orchestration function to download, parse, and check cameras.
    Also creates a 10 minute camera loop for each country,
    and construct HTML slideshows.
    """
    # save_raw saves a raw json file from the API
    # save_checked saves a json file with only online cameras
    # create_html creates an html slideshow from the json file
    default_dir = JSON_OUTPUT_DIR
    save_raw = False
    save_checked = True
    create_html = True

    # SPAIN
    spain_data = await get_camera_data("Spain", save_raw, save_checked, default_dir)
    selected_cameras = create_loop(spain_data)
    if selected_cameras and create_html:
        create_html_files(spain_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # FRANCE
    france_data = await get_camera_data("France", save_raw, save_checked, default_dir)
    selected_cameras = create_loop(france_data)
    if selected_cameras and create_html:
        create_html_files(france_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    ## ITALY
    italy_data = await get_camera_data("Italy", save_raw, save_checked, default_dir)
    selected_cameras = create_loop(italy_data)
    if selected_cameras and create_html:
        create_html_files(italy_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    ## UK
    uk_data = await get_camera_data("UK", save_raw, save_checked, default_dir)
    selected_cameras = create_loop(uk_data)
    if selected_cameras and create_html:
        create_html_files(uk_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # NETHERLANDS
    # NL only has ~25 cameras, so we just get them all
    nl_data = await get_camera_data("NL", save_raw, save_checked, default_dir)
    if create_html:
        create_html_files(nl_data, HTML_OUTPUT_DIR)


if __name__ == "__main__":
    winloop.run(main())
