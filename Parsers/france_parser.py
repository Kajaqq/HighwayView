import ast
import json
import re
import winloop
from collections import defaultdict

from tools.utils import convert_to_wgs84, save_json
from Downloaders.france_downloader import FranceDownloader
from tools.merge_france_data import merge_france_data
from config import CONSTANTS
from Parsers.base_parser import BaseParser

ROAD_REGEX = re.compile(r"\b(A|N|RN|D|M)(\d+)\b", re.IGNORECASE)
PR_REGEX = re.compile(r"PR\s*(\d+)(?:\+(\d+))?", re.IGNORECASE)
UNKNOWN_MAPPING = CONSTANTS.FRANCE.UNKNOWN_MAPPING


class FranceParser(BaseParser):
    @property
    def country(self) -> str:
        return "FR"

    def _extract_highway_name(self, text, camera_id=None):
        if text:
            match = ROAD_REGEX.search(text)
            if match:
                prefix = match.group(1).upper()
                if prefix == "RN":
                    prefix = "N"
                return f"{prefix}-{match.group(2)}"

        if camera_id and camera_id in UNKNOWN_MAPPING:
            mapped_name = UNKNOWN_MAPPING[camera_id]
            match = ROAD_REGEX.search(mapped_name)
            if match:
                prefix = match.group(1).upper()
                if prefix == "RN":
                    prefix = "N"
                return f"{prefix}-{match.group(2)}"
            return mapped_name

        return "Unknown"

    def parse_gov_cameras(self, gov_baguettes):
        try:
            raw_data = json.loads(gov_baguettes)
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error decoding Gov JSON: {e}")
            return []

        grouped_highways = defaultdict(list)
        features = raw_data.get("features") or []
        camera_sum = 0
        for feature in features:
            props = feature.get("properties") or {}
            geometry = feature.get("geometry") or {}
            full_label = props.get("libelleCamera") or ""
            camera_id = feature.get("id", "")
            camera_sum += 1

            km_point = 0.0
            pr_match = PR_REGEX.search(full_label)
            if pr_match:
                km = int(pr_match.group(1))
                meters = int(pr_match.group(2)) if pr_match.group(2) else 0
                km_point = km + (meters / 1000.0)

            flux_type = props.get("typeFlux") or ""
            cam_type = (
                "vid"
                if flux_type == "VIDEO"
                else "img"
                if flux_type == "IMAGE"
                else "unknown"
            )

            coords_in = geometry.get("coordinates") or []
            lon, lat = (
                convert_to_wgs84(coords_in[0], coords_in[1])
                if len(coords_in) >= 2
                else (0.0, 0.0)
            )

            highway_name = self._extract_highway_name(full_label, camera_id)
            camera_entry = self.format_camera(
                camera_id=camera_id,
                camera_km_point=round(km_point, 3),
                camera_view="*",
                camera_type=cam_type,
                coord_x=round(lon, 6),
                coord_y=round(lat, 6),
            )
            grouped_highways[highway_name].append(camera_entry)
        print(f"Succesfully parsed {camera_sum} gov cameras")
        # print(f"Grouped into {len(grouped_highways)} highways")
        return self.format_highway_output(grouped_highways)

    def parse_asfa_cameras(self, asfa_baguettes):
        try:
            data_string = asfa_baguettes.strip()
            data_string = re.sub(r"^var\s+\w+\s*=\s*", "", data_string)
            data_string = re.sub(r";\s*\w+\.\w+\(.*\);?$", "", data_string)
            parsed_data = ast.literal_eval(data_string)
        except Exception as e:
            print(f"Error parsing ASFA data: {e}")
            return []

        grouped_highways = defaultdict(list)
        camera_sum = 0
        for item in parsed_data:
            coords, _, _, description, metadata = item
            camera_id = metadata.get("id")
            camera_sum += 1

            highway_name = self._extract_highway_name(description, camera_id)

            camera_entry = self.format_camera(
                camera_id=camera_id,
                camera_km_point=0.0,
                camera_view="*",
                camera_type="asfa_vid",
                coord_x=coords[1],
                coord_y=coords[0],
            )

            grouped_highways[highway_name].append(camera_entry)
        print(f"Succesfully parsed {camera_sum} asfa_cameras")
        # print(f"Grouped into {len(grouped_highways)} highways")
        return self.format_highway_output(grouped_highways)

    async def parse(self, raw_data):
        asfa_raw, gov_raw = raw_data
        gov_cameras = self.parse_gov_cameras(gov_raw) if gov_raw else []
        asfa_cameras = self.parse_asfa_cameras(asfa_raw) if asfa_raw else []
        merged_data = merge_france_data(gov_cameras, asfa_cameras)
        print(f"Merged cameras grouped into {len(merged_data)} highways")
        return gov_cameras, asfa_cameras, merged_data


# Maintaining special orchestration for France due to intermediate JSON saving
async def get_parsed_data(
    output_file_gov=None,
    output_file_asfa=None,
    output_file_merged=None,
    output_folder=None,
):
    downloader = FranceDownloader()
    asfa_raw, gov_raw = await downloader.get_data()
    raw_data = asfa_raw, gov_raw
    parser = FranceParser()

    gov_cameras, asfa_cameras, merged_data = await parser.parse(raw_data)

    if output_file_gov and gov_cameras:
        save_json(gov_cameras, output_file_gov)
    if output_file_asfa and asfa_cameras:
        save_json(asfa_cameras, output_file_asfa)
    if output_file_merged:
        save_json(merged_data, output_file_merged)

    if output_folder:
        output_file_gov_name = "cameras_fr_gov.json"
        output_file_asfa_name = "cameras_fr_asfa.json"
        output_file_merged_name = "cameras_fr_merged.json"
        save_json(asfa_raw, output_folder / output_file_asfa_name)
        save_json(gov_raw, output_folder / output_file_gov_name)
        save_json(merged_data, output_folder / output_file_merged_name)

    return merged_data


if __name__ == "__main__":
    winloop.run(
        get_parsed_data(
            output_file_gov="data/cameras_fr_gov.json",
            output_file_asfa="data/cameras_fr_asfa.json",
            output_file_merged="data/cameras_fr.json",
        )
    )
