from argparse import Namespace
from pathlib import Path
from typing import Any

import winloop

from config import CONSTANTS
from Parsers import france_parser
from Parsers.be_parser import BEParser
from Parsers.italy_parser import ItalyParser
from Parsers.nl_parser import NLParser
from Parsers.spain_parser import SpainParser
from Parsers.uk_parser import UKParser
from tools.camera_check import main as camera_check
from tools.create_camera_loop import main as create_loop
from tools.create_html import main as create_html_main
from tools.utils import get_raw_parsed_data

SEP = CONSTANTS.COMMON.SEPARATOR
DEFAULT_RATE_LIMIT = CONSTANTS.COMMON.RATE_LIMIT
SPAIN_RATE_LIMIT = CONSTANTS.SPAIN.RATE_LIMIT
ITALY_RATE_LIMIT = CONSTANTS.ITALY.RATE_LIMIT
UK_RATE_LIMIT = CONSTANTS.UK.RATE_LIMIT
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL

JSON_OUTPUT_DIR: Path = CONSTANTS.COMMON.DATA_DIR
HTML_OUTPUT_DIR: Path = CONSTANTS.COMMON.HTML_DIR


def create_html_files(
    input_data: list[dict[str, Any]],
    output_dir: Path,
    camera_ids: list[str] | None = None,
    interval = DEFAULT_INTERVAL,
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


async def get_camera_data(country: str, save_unchecked: bool, save_checked: bool, output_dir: Path) -> list[dict[str, Any]]:
    """
    Downloads, parses, and explicitly checks cameras for a given country.

    Args:
        country (str): The country name (e.g., 'Spain', 'France', 'Italy', 'UK').
        save_unchecked (bool): Whether to save the unchecked JSON data.
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

    save_unchecked_path = output_dir if save_unchecked else None
    match country:
        case 'Spain':
            spain_parser = SpainParser()
            country_data = await get_raw_parsed_data(parser=spain_parser,output_path=save_unchecked_path)
            rate_limit = SPAIN_RATE_LIMIT
        case "France":
            country_data = await france_parser.get_parsed_data(output_path=save_unchecked_path)
        case "Italy":
            italy_parser = ItalyParser()
            country_data = await get_raw_parsed_data(parser=italy_parser,output_path=save_unchecked_path)
        case "UK":
            uk_parser = UKParser()
            country_data = await get_raw_parsed_data(parser=uk_parser,output_path=save_unchecked_path)
            rate_limit = UK_RATE_LIMIT
        case "NL":
            nl_parser = NLParser()
            country_data = await get_raw_parsed_data(parser=nl_parser,output_path=save_unchecked_path)
        case "BE":
            be_parser = BEParser()
            country_data = await get_raw_parsed_data(parser=be_parser,output_path=save_unchecked_path)
        case _:
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
    Also creates a 10-minute camera loop for each country
    and constructs HTML slideshows.
    """
    # save_unchecked saves a json file with all the cameras
    # save_checked saves a json file with only online cameras
    # create_html creates a html slideshow from the json file
    default_dir = JSON_OUTPUT_DIR
    save_unchecked = False
    save_checked = True
    create_html = True

    # SPAIN
    spain_data = await get_camera_data("Spain", save_unchecked, save_checked, default_dir)
    selected_cameras = create_loop(spain_data)
    if selected_cameras and create_html:
        create_html_files(spain_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # FRANCE
    france_data = await get_camera_data("France", save_unchecked, save_checked, default_dir)
    selected_cameras = create_loop(france_data)
    if selected_cameras and create_html:
        create_html_files(france_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # ITALY
    italy_data = await get_camera_data("Italy", save_unchecked, save_checked, default_dir)
    selected_cameras = create_loop(italy_data)
    if selected_cameras and create_html:
        create_html_files(italy_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # UK
    uk_data = await get_camera_data("UK", save_unchecked, save_checked, default_dir)
    selected_cameras = create_loop(uk_data)
    if selected_cameras and create_html:
        create_html_files(uk_data, HTML_OUTPUT_DIR, camera_ids=selected_cameras)

    # NETHERLANDS
    # NL only has ~25 cameras, so we just get them all
    nl_data = await get_camera_data("NL", save_unchecked, save_checked, default_dir)
    if create_html:
        create_html_files(nl_data, HTML_OUTPUT_DIR)

    # BELGIUM
    # BE has no configured highway sequence, so we just get all checked cameras.
    be_data = await get_camera_data("BE", save_unchecked, save_checked, default_dir)
    if create_html:
        create_html_files(be_data, HTML_OUTPUT_DIR)


if __name__ == "__main__":
    winloop.run(main())
