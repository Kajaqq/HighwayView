import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import winloop

from Downloaders.spain_downloader import SpainDownloader
from Parsers.base_parser import BaseParser


class SpainParser(BaseParser):
    """
    Parser for Spanish highway cameras (Source: DGT).
    """

    @property
    def country(self) -> str:
        return "ES"

    async def parse(self, raw_data: str | bytes) -> list[dict[str, Any]] | None:
        """
        Parses JSON data for Spanish highway cameras.

        Args:
            raw_data (str | bytes): The raw JSON string or bytes.

        Returns:
            list[dict[str, Any]] | None: A list of formatted highway camera dictionaries,
                or None if parsing fails.
        """
        try:
            parsed_data = json.loads(raw_data)
        except json.JSONDecodeError:
            print("Error: Failed to decode the input JSON file.")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during JSON parsing: {e}")
            return None

        try:
            grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
            camaras: list[dict[str, Any]] = parsed_data.get("camaras") or []
            for cam in camaras:
                highway_name: str = cam.get("carretera") or "Unknown"
                cam_formatted = self.format_camera(
                    camera_id=cam.get("idCamara"),
                    camera_km_point=cam.get("pk"),
                    camera_view=cam.get("sentido", "*"),
                    camera_type="img",
                    coord_x=cam.get("coordX"),
                    coord_y=cam.get("coordY"),
                )
                grouped_highways[highway_name].append(cam_formatted)

            final_output = self.format_highway_output(grouped_highways)

            print(f"Successfully parsed {len(camaras)} cameras.")
            print(f"Grouped into {len(grouped_highways)} highways")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
        else:
            return final_output


async def get_parsed_data(
    output_folder: str | Path | None = None, output_file: str | Path | None = None,
) -> Any:
    """
    Wrapper function for SpainParser.get_parsed_data.

    Args:
        output_file (str | Path | None, optional): Specific file path to save output. Defaults to None.
        output_folder (str | Path | None, optional): Folder to save output according to country format. Defaults to None.

    Returns:
        Any: The parsed camera data.
    """
    parser = SpainParser(downloader=SpainDownloader())
    return await parser.get_parsed_data(output_folder=output_folder, output_file=output_file)


if __name__ == "__main__":
    output = Path('data')
    winloop.run(
        get_parsed_data(output_folder=output)
    )
