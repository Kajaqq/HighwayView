import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import winloop
from lxml import html

from Downloaders.be_downloader import BEDownloader
from Parsers.base_parser import BaseParser
from tools.utils import load_json

WEB_MERCATOR_RADIUS_M = 6378137.0
HIGHWAY_RE = re.compile(r"\b[AENR]\d{1,4}\b")


class BEParser(BaseParser):
    """
    Parser for Belgian highway cameras (Vlaams Verkeerscentrum).
    """

    def __init__(self) -> None:
        super().__init__(BEDownloader())

    @property
    def country(self) -> str:
        return "BE"

    @staticmethod
    def _web_mercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
        lon = math.degrees(x / WEB_MERCATOR_RADIUS_M)
        lat = math.degrees(2 * math.atan(math.exp(y / WEB_MERCATOR_RADIUS_M)) - math.pi / 2)
        return lon, lat

    @classmethod
    def _extract_coords(cls, camera: dict[str, Any]) -> tuple[float | None, float | None]:
        coordinates = camera.get("location", {}).get("coordinates", [])
        if len(coordinates) < 2:
            return None, None

        try:
            return cls._web_mercator_to_wgs84(float(coordinates[0]), float(coordinates[1]))
        except (TypeError, ValueError):
            return None, None

    @staticmethod
    def _extract_modal_title(popup_html: str) -> str:
        tree = html.fromstring(popup_html)
        title = tree.xpath(
            'normalize-space(string(.//h3[contains(concat(" ", normalize-space(@class), " "), " modal-title ")]))'
        )
        return title or "Unknown"

    @staticmethod
    def _normalize_internal_name(value: str) -> str:
        name = value.strip()
        for suffix in (".stream.jpg", ".stream.jpeg", ".stream.png", ".stream"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
        return name

    @classmethod
    def _extract_internal_name(cls, popup_html: str) -> str:
        tree = html.fromstring(popup_html)
        media_urls = tree.xpath(".//iframe/@src") + tree.xpath(".//img/@src")

        for media_url in media_urls:
            parsed_url = urlparse(str(media_url))
            names = parse_qs(parsed_url.query).get("name")
            if names:
                return cls._normalize_internal_name(names[0])

            filename = Path(parsed_url.path).name
            if filename:
                return cls._normalize_internal_name(filename)

        return ""

    @staticmethod
    def _infer_highway(modal_title: str) -> str:
        highway = HIGHWAY_RE.search(modal_title)
        return highway.group(0) if highway else "Unknown"

    async def parse(self, raw_data: str | bytes | list[dict[str, Any]]) -> list[dict[str, Any]]:
        camera_data: list[dict[str, Any]] = load_json(raw_data)
        camera_ids = [camera.get("nid") for camera in camera_data if camera.get("nid")]
        popup_data = await self.downloader.get_popup_data(camera_ids)

        grouped_highways: dict[str, list[dict[str, Any]]] = defaultdict(list)
        skipped_count = 0

        for camera in camera_data:
            nid = str(camera.get("nid", ""))
            popup_html = popup_data.get(nid)
            if not popup_html:
                skipped_count += 1
                continue

            modal_title = self._extract_modal_title(popup_html)
            internal_name = self._extract_internal_name(popup_html)
            if not internal_name:
                skipped_count += 1
                continue

            lon, lat = self._extract_coords(camera)
            highway_name = self._infer_highway(modal_title)
            camera_entry = self.format_camera(
                camera_id=internal_name,
                camera_km_point=0.0,
                camera_view="*",
                camera_type="vid",
                coord_x=lon,
                coord_y=lat,
            )
            grouped_highways[highway_name].append(camera_entry)

        final_output = self.format_highway_output(grouped_highways)
        parsed_count = sum(len(item["highway"]["cameras"]) for item in final_output)
        print(f"Successfully parsed {parsed_count} Belgium cameras.")
        if skipped_count:
            print(f"Skipped {skipped_count} Belgium cameras without usable popup data.")
        print(f"Grouped into {len(grouped_highways)} highways")
        return final_output


if __name__ == "__main__":
    output_folder = Path(__file__).parent.parent / "data"
    parser = BEParser()
    winloop.run(parser.get_parsed_data(output_path=output_folder))
