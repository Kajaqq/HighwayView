from collections import defaultdict
from pathlib import Path
from typing import Any

import winloop

from Downloaders.nl_downloader import NLDownloader
from Parsers.base_parser import BaseParser
from tools.utils import load_json


class NLParser(BaseParser):
    """
    Parser for the Netherlands highway cameras (Rijkswaterstaat).
    """
    def __init__(self):
        super().__init__()
        self.downloader = NLDownloader()

    @property
    def country(self) -> str:
        return "NL"

    @staticmethod
    def stream_id(stream_url: str) -> str:
        path_parts = stream_url.split("/")
        stream_id = path_parts[3]
        return stream_id

    async def parse(self, raw_data: str | bytes | list[dict[str, Any]]) -> list[dict[str, Any]]:
        camera_data: list[dict[str, Any]] = load_json(raw_data)
        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for camera in camera_data:
            highway_name = camera.get("road") or "Unknown"
            stream_url = camera.get("stream_url") or ""
            static_url = camera.get("static_url") or ""
            camera_id = self.stream_id(stream_url or static_url)
            lon = camera.get("longitude")
            lat = camera.get("latitude")
            camera_entry = self.format_camera(
                camera_id=camera_id,
                camera_km_point=0.0,
                camera_view="*",
                camera_type="iframe",
                coord_x=lon,
                coord_y=lat,
            )
            grouped_highways[highway_name].append(camera_entry)

        final_output = self.format_highway_output(grouped_highways)
        print(f"Successfully parsed {len(camera_data)} cameras.")
        print(f"Grouped into {len(grouped_highways)} highways")
        return final_output


if __name__ == "__main__":
    output_folder = Path(__file__).parent.parent / "data"
    parser = NLParser()
    winloop.run(parser.get_parsed_data(output_path=output_folder))
