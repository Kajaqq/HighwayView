import winloop
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from tools.utils import load_json
from config import CONSTANTS
from Downloaders.italy_downloader import ItalyDownloader
from Parsers.base_parser import BaseParser

CAMERA_BASE_URL: str = CONSTANTS.ITALY.CAMERA_URL


class ItalyParser(BaseParser):
    """
    Parser for Italian highway cameras.

    Handles data from various Italian sources including Autostrade, A22,
    and different A4 sections (ABP, CAV, SATAP).
    """

    @property
    def country(self) -> str:
        """
        Property that returns the country code.

        Returns:
            str: The two-letter country code ('IT').
        """
        return "IT"

    def parse_autostrade_cameras(self, raw_data: str | bytes) -> list[dict[str, Any]]:
        """
        Parses JSON data for Autostrade cameras. (Most of the country cameras are here)

        Args:
            raw_data (str | bytes): The raw JSON string or bytes containing Autostrade data.

        Returns:
            list[dict[str, Any]]: A list of formatted highway camera dictionaries.
        """
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
        except Exception as e:
            print(f"Error parsing Autostrade JSON: {e}")
            return []

        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
        webcams: list[dict[str, Any]] = data.get("webcams", [])

        for cam in webcams:
            highway_name: str = cam.get("c_str", "Unknown")
            if highway_name == "A4":
                highway_name = "A04"

            video_fragment: str = cam.get("frames", {}).get("V", {}).get("t_url", "")
            if not video_fragment:
                continue

            full_url: str = f"{CAMERA_BASE_URL}{video_fragment}"

            km_ini: float | None = cam.get("n_prg_km_ini")
            km_fin: float | None = cam.get("n_prg_km_fin")

            direction = "*"
            if km_ini is not None and km_fin is not None:
                if km_ini < km_fin:
                    direction = "+"
                elif km_ini > km_fin:
                    direction = "-"

            camera_entry = self.format_camera(
                camera_id=cam.get("c_tel"),
                camera_km_point=cam.get("n_prg_km", 0.0),
                camera_view=direction,
                camera_type="vid",
                coord_x=cam.get("n_crd_lon"),
                coord_y=cam.get("n_crd_lat"),
                url=full_url,
            )
            grouped_highways[highway_name].append(camera_entry)

        return self.format_highway_output(grouped_highways)

    def parse_a22_cameras(self, raw_data: str | bytes) -> list[dict[str, Any]]:
        """
        Parses JSON data for A22 highway cameras. (Brenner Pass)

        Args:
            raw_data (str | bytes): The raw JSON string or bytes for A22 data.

        Returns:
            list[dict[str, Any]]: A list containing the formatted A22 highway camera dictionary.
        """
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
        except Exception as e:
            print(f"Error parsing A22 JSON: {e}")
            return []

        cameras: list[dict[str, Any]] = []
        for region in data.values():
            for cam in region:
                desc: str = cam.get("Descrizione", "").lower()
                direction = "*"
                if "modena" in desc:
                    direction = "+"
                elif "brennero" in desc:
                    direction = "-"

                img_url: str = cam.get("Immagine", "")
                if img_url.startswith("//"):
                    img_url = "https:" + img_url

                camera_entry = self.format_camera(
                    camera_id=cam.get("ID"),
                    camera_km_point=cam.get("Distanza", 0.0),
                    camera_view=direction,
                    camera_type="img",
                    coord_x=cam.get("Lng"),
                    coord_y=cam.get("Lat"),
                    url=img_url,
                )
                cameras.append(camera_entry)

        return (
            [{"highway": {"name": "A22", "country": self.country, "cameras": cameras}}]
            if cameras
            else []
        )

    def parse_a4_abp(self, raw_data: str | bytes) -> list[dict[str, Any]]:
        """
        Parses JSON data for the A4 ABP section cameras.

        Args:
            raw_data (str | bytes): The raw JSON string or bytes.

        Returns:
            list[dict[str, Any]]: A list of formatted camera dictionaries for this section.
        """
        base_url: str = CONSTANTS.ITALY.A4.ABP.BASE_ABP_URL
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
            cameras: list[dict[str, Any]] = []
            for cam in data:
                km_start: int = cam["name"].find("km")
                if km_start != -1:
                    km_from_name: str = cam["name"][km_start + 2 : km_start + 8].strip()
                    # Filter just digits and dots
                    km_from_name = "".join(
                        c for c in km_from_name if c.isdigit() or c == "."
                    )
                    km_point: float = float(km_from_name) if km_from_name else 0.0
                else:
                    km_point = 0.0
                video_url: str = cam.get("url", "")
                video_url = base_url + video_url

                if not video_url:
                    continue

                camera_entry = self.format_camera(
                    camera_id=cam.get("id", ""),
                    camera_km_point=km_point,
                    camera_view="*",
                    camera_type="vid"
                    if video_url.endswith((".mp4", ".m3u8"))
                    else "img",
                    coord_x=cam.get("lng"),
                    coord_y=cam.get("lat"),
                    url=video_url,
                )
                cameras.append(camera_entry)
        except Exception as e:
            print(f"Error parsing A4 ABP data: {e}")
            return []
        else:
            return cameras

    def parse_a4_cav(self, raw_data: str | bytes) -> list[dict[str, Any]]:
        """
        Parses JSON data for the A4 CAV section cameras.

        Args:
            raw_data (str | bytes): The raw JSON string or bytes.

        Returns:
            list[dict[str, Any]]: A list of formatted camera dictionaries for this section.
        """
        if not raw_data:
            return []
        base_ip_url: str = CONSTANTS.ITALY.A4.CAV.WEBCAM_URL
        try:
            json_file = load_json(raw_data)
            file_features: list[dict[str, Any]] = json_file.get("features", [])
            cameras: list[dict[str, Any]] = []

            for feature in file_features:
                camera_data: dict[str, Any] = feature.get("properties", {})
                cam_url: str = camera_data.get("URL", "")
                if cam_url.startswith("https://inviaggio.autobspd.it/"):
                    continue

                if (
                    camera_data.get("VIS_WEB") == "S"
                ):  # If camera is visible/online. Italy actually checks their cameras.
                    if cam_url == "---":
                        # Italy calls the camera id -> 'IP" for some reason
                        cam_url = base_ip_url.format(ip=camera_data.get("IP", ""))

                    geometry: dict[str, Any] = feature.get("geometry", {})
                    coords: list[Any] = geometry.get("coordinates", [None, None])

                    camera_entry = self.format_camera(
                        camera_id=camera_data.get("IDTELECAMERA", ""),
                        camera_km_point=camera_data.get("PROG_KM", 0.0),
                        camera_view="*",
                        camera_type="img",
                        coord_x=coords[0] if len(coords) > 0 else None,
                        coord_y=coords[1] if len(coords) > 1 else None,
                        url=cam_url,
                    )
                    cameras.append(camera_entry)
        except Exception as e:
            print(f"Error parsing A4 CAV data: {e}")
            return []
        else:
            return cameras

    def parse_a4_satap(self, raw_data: str) -> list[dict[str, Any]]:
        """
        Parses HTML data for the A4 SATAP section cameras using regex.

        Args:
            raw_data (str): The raw HTML string.

        Returns:
            list[dict[str, Any]]: A list of formatted camera dictionaries for this section.
        """
        if not raw_data:
            return []
        keyword_start: str = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[0]
        keyword_end: str = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[1]

        try:
            blocks = re.findall(
                f"{re.escape(keyword_start)}(.*?){re.escape(keyword_end)}",
                raw_data,
                re.DOTALL,
            )

            cameras: list[dict[str, Any]] = []
            for block in blocks:
                title_match = re.search(r"<h2>(.*?)</h2>", block)
                title = title_match.group(1).strip() if title_match else "Unknown"

                km_point = 0.0
                km_match = re.search(r"KM\s*(\d+)\+(\d+)", title)
                if km_match:
                    km_point = (
                        float(km_match.group(1)) + float(km_match.group(2)) / 1000
                    )

                video_match = re.search(r'href="(https?://[^"]+\.mp4)"', block)
                video_url = video_match.group(1) if video_match else None

                if not video_url:
                    continue

                cam_id = video_url.split("/")[-1].split(".")[0]

                camera_entry = self.format_camera(
                    camera_id=cam_id,
                    camera_km_point=km_point,
                    camera_view="*",
                    camera_type="vid",
                    coord_x=None,
                    coord_y=None,
                    url=video_url,
                )
                cameras.append(camera_entry)
        except Exception as e:
            print(f"Error parsing A4 SATAP data: {e}")
            return []
        else:
            return cameras

    async def parse(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Parses data from all Italian sources and merges them into a single list.

        Args:
            raw_data (dict[str, Any]): A dictionary mapping data sources to their raw content.

        Returns:
            list[dict[str, Any]]: The merged and deduplicated list of Italian highway cameras.
        """
        parsed_data = self.parse_autostrade_cameras(raw_data.get("autostrade", ""))

        a22_parsed = self.parse_a22_cameras(raw_data.get("a22", ""))
        if a22_parsed:
            parsed_data.extend(a22_parsed)

        a4_cameras: list[dict[str, Any]] = []

        # As Italy has various concessionaries for the main road, we need to handle the government + 3 other sections.
        a4_cameras.extend(self.parse_a4_abp(raw_data.get("a4_abp", "")))
        a4_cameras.extend(self.parse_a4_cav(raw_data.get("a4_cav", "")))
        a4_cameras.extend(self.parse_a4_satap(raw_data.get("a4_satap", "")))

        if a4_cameras:
            a4_entry = next(
                (h for h in parsed_data if h["highway"]["name"] == "A04"), None
            )
            if a4_entry:
                a4_entry["highway"]["cameras"].extend(a4_cameras)
            else:
                # This shouldn't happen, but just in case
                parsed_data.append(
                    {
                        "highway": {
                            "name": "A04",
                            "country": self.country,
                            "cameras": a4_cameras,
                        }
                    }
                )

        # De-duplicate cameras based on coordinates
        parsed_data = self.merge_camera_data(
            parsed_data,
            match_by="coordinates",
            threshold=0.015,
            check_id=True,
            check_url=True,
        )

        total_cameras = sum(len(h["highway"]["cameras"]) for h in parsed_data)
        print(f"Successfully parsed {total_cameras} cameras.")
        print(f"Grouped into {len(parsed_data)} highways")
        return parsed_data


async def get_parsed_data(
    output_file: str | Path | None = None, output_folder: str | Path | None = None
) -> Any:
    """
    Wrapper function for ItalyParser.get_parsed_data.

    Args:
        output_file (str | Path | None, optional): Specific file path to save output. Defaults to None.
        output_folder (str | Path | None, optional): Folder to save output according to country format. Defaults to None.

    Returns:
        Any: The parsed camera data.
    """
    parser = ItalyParser(downloader=ItalyDownloader())
    return await parser.get_parsed_data(
        output_file=output_file, output_folder=output_folder
    )


if __name__ == "__main__":
    output = Path(__file__).parent.parent / "data" / "cameras_it.json"
    winloop.run(get_parsed_data(output))
