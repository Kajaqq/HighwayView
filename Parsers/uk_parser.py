from collections import defaultdict
from pathlib import Path
from typing import Any

import winloop

from Downloaders.uk_downloader import UKDownloader
from Parsers.base_parser import BaseParser
from tools.utils import load_json


class UKParser(BaseParser):
    """
    Parser for UK highway cameras (Traffic England).
    """

    def __init__(self):
        super().__init__()
        self.downloader=UKDownloader()

    @property
    def country(self) -> str:
        return "UK"

    async def parse(self, raw_data: str | bytes) -> list[dict[str, Any]]:
        """
        Parses JSON data for UK highway cameras.

        Args:
            raw_data (str | bytes): The raw JSON string or bytes.

        Returns:
            list[dict[str, Any]]: A list of formatted highway camera dictionaries.
        """
        camera_data: list[dict[str, Any]] = load_json(raw_data)
        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for cam in camera_data:
            cam_desc: list[str] = cam.get("description", "").split(" ")
            highway_name: str = cam_desc[0] if len(cam_desc) > 0 else "Unknown"
            camera_id: str = cam_desc[1] if len(cam_desc) > 1 else ""
            # UK data unfortunately doesn't have km_point or camera_view as they operate on a different standard
            cam_formatted = self.format_camera(
                camera_id=camera_id,
                camera_km_point=0.0,
                camera_view="*",
                camera_type="img",
                coord_x=cam.get("longitude"),
                coord_y=cam.get("latitude"),
            )
            grouped_highways[highway_name].append(cam_formatted)

        final_output = self.format_highway_output(grouped_highways)
        print(f"Successfully parsed {len(camera_data)} cameras.")
        print(f"Grouped into {len(grouped_highways)} highways")
        return final_output




if __name__ == "__main__":
    parser = UKParser()
    winloop.run(parser.get_parsed_data(output_path=Path("data")))
