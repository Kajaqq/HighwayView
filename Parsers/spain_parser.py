import json
from collections import defaultdict
from pathlib import Path
import asyncio

from Downloaders.spain_downloader import SpainDownloader
from Parsers.base_parser import BaseParser


class SpainParser(BaseParser):
    @property
    def country(self) -> str:
        return "ES"

    async def parse(self, raw_data):
        try:
            raw_data = json.loads(raw_data)
        except json.JSONDecodeError:
            print("Error: Failed to decode the input JSON file.")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during JSON parsing: {e}")
            return None

        try:
            grouped_highways = defaultdict(list)
            camaras = raw_data.get("camaras") or []
            for cam in camaras:
                highway_name = cam.get("carretera") or "Unknown"
                cam_formatted = self.format_camera(
                    camera_id=cam.get("idCamara"),
                    camera_km_point=cam.get("pk"),
                    camera_view=cam.get("sentido"),
                    camera_type="img",
                    coord_x=cam.get("coordX"),
                    coord_y=cam.get("coordY"),
                )
                grouped_highways[highway_name].append(cam_formatted)

            final_output = self.format_highway_output(grouped_highways)

            print(f"Successfully parsed {len(camaras)} cameras.")
            print(f"Grouped into {len(grouped_highways)} highways")
            return final_output

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None


async def get_parsed_data(output_file=None, output_folder=None):
    parser = SpainParser(downloader=SpainDownloader())
    return await parser.get_parsed_data(
        output_file=output_file, output_folder=output_folder
    )


if __name__ == "__main__":
    asyncio.run(get_parsed_data(output_folder=Path("../data")))
