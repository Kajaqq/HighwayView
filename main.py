import asyncio
from argparse import Namespace

from Parsers import france_parser, italy_parser, spain_parser, uk_parser
from tools.camera_check import main as camera_check
from config import CONSTANTS
from tools.create_camera_loop import filter_cameras as create_loop
from tools.create_html import main as create_html_main

SEP = CONSTANTS.COMMON.SEPARATOR
DEFAULT_RATE_LIMIT = CONSTANTS.COMMON.RATE_LIMIT
SPAIN_RATE_LIMIT = CONSTANTS.SPAIN.RATE_LIMIT
ITALY_RATE_LIMIT = CONSTANTS.ITALY.RATE_LIMIT
UK_RATE_LIMIT = CONSTANTS.UK.RATE_LIMIT
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL

JSON_OUTPUT_DIR = CONSTANTS.COMMON.DATA_DIR
HTML_OUTPUT_DIR = CONSTANTS.COMMON.HTML_DIR

SPAIN_LOOP = CONSTANTS.SPAIN.HIGHWAY_SEQUENCE
FRANCE_LOOP = CONSTANTS.FRANCE.HIGHWAY_SEQUENCE


def create_html_files(input_data, output_dir, interval=DEFAULT_INTERVAL):
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
        camera_ids=None,
        highways=None,
        interval=interval,
        no_sort=True,
        include_unknown=False,
    )
    create_html_main(args)


async def get_camera_data(country, save_raw, save_checked, output_dir):
    print(SEP)
    print(f"Downloading {country} data...")
    print(SEP)
    rate_limit = DEFAULT_RATE_LIMIT
    if save_raw:
        save_raw = output_dir

    if country == "Spain":
        country_data = await spain_parser.get_parsed_data(save_raw)
        rate_limit = SPAIN_RATE_LIMIT

    elif country == "France":
        country_data = await france_parser.get_parsed_data(output_folder=save_raw)

    elif country == "Italy":
        country_data = await italy_parser.get_parsed_data(output_folder=save_raw)

    elif country == "UK":
        country_data = await uk_parser.get_parsed_data(output_folder=save_raw)
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


async def main():
    # save_raw saves a raw json file from the API
    # save_checked saves a json file with only online cameras
    # create_html creates an html slideshow from the json file
    default_dir = JSON_OUTPUT_DIR
    save_raw = False
    save_checked = False
    save_loop = True
    create_html = False

    # SPAIN
    spain_data = await get_camera_data("Spain", save_raw, save_checked, default_dir)
    looped_data = create_loop(input_data=spain_data, save_loop=save_loop)
    if looped_data and create_html:
        create_html_files(looped_data, HTML_OUTPUT_DIR)

    # FRANCE
    france_data = await get_camera_data("France", save_raw, save_checked, default_dir)
    looped_data = create_loop(input_data=france_data, save_loop=save_loop)
    if looped_data and create_html:
        create_html_files(looped_data, HTML_OUTPUT_DIR)

    ## ITALY
    italy_data = await get_camera_data("Italy", save_raw, save_checked, default_dir)
    looped_data = create_loop(input_data=italy_data, save_loop=save_loop)
    if looped_data and create_html:
        create_html_files(looped_data, HTML_OUTPUT_DIR)

    ## UK
    await get_camera_data("UK", save_raw, save_checked, default_dir)


if __name__ == "__main__":
    asyncio.run(main())
