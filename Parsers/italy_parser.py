import winloop
import re
from collections import defaultdict
from pathlib import Path

from tools.utils import load_json
from config import CONSTANTS
from Downloaders.italy_downloader import ItalyDownloader
from Parsers.base_parser import BaseParser

CAMERA_BASE_URL = CONSTANTS.ITALY.CAMERA_URL


class ItalyParser(BaseParser):
    @property
    def country(self) -> str:
        return "IT"

    def parse_autostrade_cameras(self, raw_data):
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
        except Exception as e:
            print(f"Error parsing Autostrade JSON: {e}")
            return []

        grouped_highways = defaultdict(list)
        webcams = data.get("webcams", [])

        for cam in webcams:
            highway_name = cam.get("c_str", "Unknown")
            if highway_name == "A4":
                highway_name = "A04"

            video_fragment = cam.get("frames", {}).get("V", {}).get("t_url", "")
            if not video_fragment:
                continue

            full_url = f"{CAMERA_BASE_URL}{video_fragment}"

            km_ini = cam.get("n_prg_km_ini")
            km_fin = cam.get("n_prg_km_fin")

            if km_ini is not None and km_fin is not None:
                if km_ini < km_fin:
                    direction = "+"
                elif km_ini > km_fin:
                    direction = "-"
                else:
                    direction = "*"
            else:
                direction = "*"

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

    def parse_a22_cameras(self, raw_data):
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
        except Exception as e:
            print(f"Error parsing A22 JSON: {e}")
            return []

        cameras = []
        for region in data.values():
            for cam in region:
                desc = cam.get("Descrizione", "").lower()
                if "modena" in desc:
                    direction = "+"
                elif "brennero" in desc:
                    direction = "-"
                else:
                    direction = "*"

                img_url = cam.get("Immagine", "")
                if img_url.startswith("//"):
                    img_url = "https:" + img_url

                camera_entry = self.format_camera(
                    camera_id=cam.get("ID"),
                    camera_km_point=cam.get("Distanza", 0),
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

    def parse_a4_abp(self, raw_data):
        BASE_URL = CONSTANTS.ITALY.A4.ABP.BASE_ABP_URL
        if not raw_data:
            return []
        try:
            data = load_json(raw_data)
            cameras = []
            for cam in data:
                km_start = cam["name"].find("km")
                km_from_name = cam["name"][km_start + 2 : km_start + 6].strip()
                km_point = float(km_from_name) if km_from_name.isdigit() else 0.0
                video_url = cam.get("url", "")
                video_url = BASE_URL + video_url

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

    def parse_a4_cav(self, raw_data):
        if not raw_data:
            return []
        base_ip_url = CONSTANTS.ITALY.A4.CAV.WEBCAM_URL
        try:
            json_file = load_json(raw_data)
            file_features = json_file.get("features", [])
            cameras = []

            for feature in file_features:
                camera_data = feature.get("properties", {})
                cam_url = camera_data.get("URL", "")
                if cam_url.startswith("https://inviaggio.autobspd.it/"):
                    continue
                if camera_data.get("VIS_WEB") == "S":
                    if cam_url == "---":
                        cam_url = base_ip_url.format(ip=camera_data.get("IP", ""))

                    geometry = feature.get("geometry", {})
                    coords = geometry.get("coordinates", [None, None])

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

    def parse_a4_satap(self, raw_data):
        if not raw_data:
            return []
        keyword_start = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[0]
        keyword_end = CONSTANTS.ITALY.A4.SATAP.CAMERA_KEYWORDS[1]

        try:
            blocks = re.findall(
                f"{re.escape(keyword_start)}(.*?){re.escape(keyword_end)}",
                raw_data,
                re.DOTALL,
            )

            cameras = []
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

    async def parse(self, raw_data):
        parsed_data = self.parse_autostrade_cameras(raw_data["autostrade"])

        a22_parsed = self.parse_a22_cameras(raw_data["a22"])
        if a22_parsed:
            parsed_data.extend(a22_parsed)

        a4_cameras = []
        a4_cameras.extend(self.parse_a4_abp(raw_data["a4_abp"]))
        a4_cameras.extend(self.parse_a4_cav(raw_data["a4_cav"]))
        a4_cameras.extend(self.parse_a4_satap(raw_data["a4_satap"]))

        if a4_cameras:
            a4_entry = next(
                (h for h in parsed_data if h["highway"]["name"] == "A04"), None
            )
            if a4_entry:
                a4_entry["highway"]["cameras"].extend(a4_cameras)
            else:
                parsed_data.append(self.format_highway_output({"A04": a4_cameras}))

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


async def get_parsed_data(output_file=None, output_folder=None):
    parser = ItalyParser(downloader=ItalyDownloader())
    return await parser.get_parsed_data(
        output_file=output_file, output_folder=output_folder
    )


if __name__ == "__main__":
    output = Path(__file__).parent.parent / "data" / "cameras_it.json"
    winloop.run(get_parsed_data(output))
