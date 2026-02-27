import winloop
from collections import defaultdict

from Downloaders.uk_downloader import UKDownloader
from tools.utils import load_json
from Parsers.base_parser import BaseParser


class UKParser(BaseParser):
    @property
    def country(self) -> str:
        return "UK"

    async def parse(self, raw_data):
        camera_data = load_json(raw_data)
        grouped_highways = defaultdict(list)
        for cam in camera_data:
            cam_desc = cam.get("description", "").split(" ")
            highway_name = cam_desc[0] if len(cam_desc) > 0 else "Unknown"
            camera_id = cam_desc[1] if len(cam_desc) > 1 else ""

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


# Maintaining backward compatibility
async def get_parsed_data(output_file=None, output_folder=None):
    parser = UKParser(downloader=UKDownloader())
    return await parser.get_parsed_data(
        output_file=output_file, output_folder=output_folder
    )


if __name__ == "__main__":
    winloop.run(get_parsed_data())
