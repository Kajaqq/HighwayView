import ast
import json
import re
import winloop
from collections import defaultdict
from typing import Any
from pathlib import Path

from tools.utils import convert_to_wgs84, save_json
from Downloaders.france_downloader import FranceDownloader
from config import CONSTANTS
from Parsers.base_parser import BaseParser

ROAD_REGEX = re.compile(r"\b(A|N|RN|D|M)(\d+)\b", re.IGNORECASE)
PR_REGEX = re.compile(r"PR\s*(\d+)(?:\+(\d+))?", re.IGNORECASE)
UNKNOWN_MAPPING: dict[str, str] = CONSTANTS.FRANCE.UNKNOWN_MAPPING


class FranceParser(BaseParser):
    """
    Parser for French highway cameras.

    Handles data from two sources:
    1. Deobfuscated government GeoJSON data (Bison Futé).
    2. ASFA (Association des Sociétés Françaises d'Autoroutes) Javascript array data.
    """

    @property
    def country(self) -> str:
        """
        Property that returns the country code.

        Returns:
            str: The two-letter country code ('FR').
        """
        return "FR"

    @staticmethod
    def _extract_highway_name(text: str, camera_id: str | None = None) -> str:
        """
        Extracts a normalized highway name from raw text or falls back to a
        known mapping using the camera ID.

        Args:
            text (str): The raw text description containing the highway name.
            camera_id (str | None, optional): The ID of the camera, used for fallback
                mapping if the text doesn't explicitly match the road regex. Defaults to None.

        Returns:
            str: The normalized highway name (e.g., 'A-10', 'N-154') or 'Unknown'.
        """
        def _normalize_rn(prefix_:str):
            # France national highways 'Route Nationale' use RN or N interchangeably. We normalize it to more common 'N'
            return "N" if prefix == "RN" else prefix

        if text:
            # Try to get the first apperance of a highway name pattern
            match = ROAD_REGEX.search(text)
            if match:
                prefix = match.group(1).upper()
                prefix = _normalize_rn(prefix)
                return f"{prefix}-{match.group(2)}"

        # This is needed as France metadata sometimes doesn't include the specific name
        # The list has been manually checked based on coordinates
        if camera_id and camera_id in UNKNOWN_MAPPING:
            mapped_name = UNKNOWN_MAPPING[camera_id]
            match = ROAD_REGEX.search(mapped_name)
            if match:
                prefix = match.group(1).upper()
                prefix =  _normalize_rn(prefix)
                return f"{prefix}-{match.group(2)}"
            return mapped_name

        return "Unknown"

    def parse_gov_cameras(self, gov_baguettes: str | bytes) -> list[dict[str, Any]]:
        """
        Parses raw GeoJSON data from the French government (Bison Futé).

        Args:
            gov_baguettes (str | bytes): The raw GeoJSON string or bytes.

        Returns:
            list[dict[str, Any]]: A list of formatted highway camera dictionaries.
        """
        def _km_point_get(full_label_) -> float:
            km_pt = 0.0
            pr_match = PR_REGEX.search(full_label_)
            if pr_match:
                km = int(pr_match.group(1))
                meters_str = pr_match.group(2)
                meters = int(meters_str) if meters_str else 0
                km_pt = km + (meters / 1000.0)
            return km_pt

        try:
            raw_data = json.loads(gov_baguettes)
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error decoding Gov JSON: {e}")
            return []

        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
        features: list[dict[str, Any]] = raw_data.get("features") or []
        camera_sum = 0

        for feature in features:
            camera_sum += 1
            props: dict[str, Any] = feature.get("properties") or {}
            geometry: dict[str, Any] = feature.get("geometry") or {}
            full_label: str = props.get("libelleCamera") or ""
            camera_id: str = feature.get("id", "")
            km_point = _km_point_get(full_label)
            flux_type: str = props.get("typeFlux") or ""
            cam_type = (
                "vid"
                if flux_type == "VIDEO"
                else "img"
                if flux_type == "IMAGE"
                else "unknown"
            )

            coords_in: list[float] = geometry.get("coordinates") or []
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

        print(f"Succesfully parsed {camera_sum} Government cameras")
        return self.format_highway_output(grouped_highways)

    def parse_asfa_cameras(self, asfa_baguettes: str) -> list[dict[str, Any]]:
        """
        Parses raw Javascript array data from ASFA (Autoroutes.fr).

        Args:
            asfa_baguettes (str): The raw JavaScript code snippet containing camera data.

        Returns:
            list[dict[str, Any]]: A list of formatted highway camera dictionaries.
        """
        try:
            data_string = asfa_baguettes.strip()
            data_string = re.sub(r"^var\s+\w+\s*=\s*", "", data_string)
            data_string = re.sub(r";\s*\w+\.\w+\(.*\);?$", "", data_string)
            parsed_data: list[Any] = ast.literal_eval(data_string)
        except Exception as e:
            print(f"Error parsing ASFA data: {e}")
            return []

        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
        camera_sum = 0
        for item in parsed_data:
            coords, _, _, description, metadata = item
            camera_id: str = metadata.get("id")
            camera_sum += 1

            highway_name = self._extract_highway_name(description, camera_id)

            # ASFA doesn't return km_point or camera direction
            camera_entry = self.format_camera(
                camera_id=camera_id,
                camera_km_point=0.0,
                camera_view="*",
                camera_type="asfa_vid",
                coord_x=float(coords[1]),
                coord_y=float(coords[0]),
            )

            grouped_highways[highway_name].append(camera_entry)

        print(f"Succesfully parsed {camera_sum} ASFA cameras")
        return self.format_highway_output(grouped_highways)

    async def parse(
        self, raw_data: tuple[str, str | bytes]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Parses both ASFA and Government raw datasets and merges them.

        Args:
            raw_data (tuple[str, str | bytes]): A tuple containing the raw ASFA string
                and raw Government GeoJSON string/bytes respectively.

        Returns:
            tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
                A tuple holding:
                - The list of parsed Government highway cameras.
                - The list of parsed ASFA highway cameras.
                - The list of merged and deduplicated highway cameras.
        """
        asfa_raw, gov_raw = raw_data
        gov_cameras = self.parse_gov_cameras(gov_raw) if gov_raw else []
        asfa_cameras = self.parse_asfa_cameras(asfa_raw) if asfa_raw else []
        merged_data = self.merge_camera_data(
            gov_cameras, asfa_cameras, match_by="coordinates", threshold=0.20
        )
        print(f"Merged cameras grouped into {len(merged_data)} highways")
        return gov_cameras, asfa_cameras, merged_data


async def get_parsed_data(
    output_file_gov: str | Path | None = None,
    output_file_asfa: str | Path | None = None,
    output_file_merged: str | Path | None = None,
    output_folder: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Downloads, parses, and merges French camera data from multiple sources.

    This function handles special intermediate JSON saving requirements
    unique to the French datasets.

    Args:
        output_file_gov (str | Path | None, optional): File path for saving parsed Gov data.
        output_file_asfa (str | Path | None, optional): File path for saving parsed ASFA data.
        output_file_merged (str | Path | None, optional): File path for saving merged data.
        output_folder (str | Path | None, optional): Folder to save raw files and defaults.

    Returns:
        list[dict[str, Any]]: The merged list of French highway camera data.
    """
    downloader = FranceDownloader()
    asfa_raw, gov_raw = await downloader.get_data()
    raw_data = asfa_raw, gov_raw
    parser = FranceParser()

    gov_cameras, asfa_cameras, merged_data = await parser.parse(raw_data)

    if output_file_merged:
        save_json(merged_data, output_file_merged)

    # Allow saving camera sources separately
    if output_file_gov and gov_cameras:
        save_json(gov_cameras, output_file_gov)
    if output_file_asfa and asfa_cameras:
        save_json(asfa_cameras, output_file_asfa)

    if output_folder:
        # If output folder is specified, save all files
        folder_path = Path(output_folder)
        output_file_gov_name = "cameras_fr_gov.json"
        output_file_asfa_name = "cameras_fr_asfa.json"
        output_file_merged_name = "cameras_fr_merged.json"
        save_json(asfa_raw, folder_path / output_file_asfa_name)
        save_json(gov_raw, folder_path / output_file_gov_name)
        save_json(merged_data, folder_path / output_file_merged_name)

    return merged_data


if __name__ == "__main__":
    winloop.run(
        get_parsed_data(
            output_file_gov="data/cameras_fr_gov.json",
            output_file_asfa="data/cameras_fr_asfa.json",
            output_file_merged="data/cameras_fr.json",
        )
    )
