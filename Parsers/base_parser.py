import copy
import inspect
from abc import ABC, abstractmethod
from pathlib import Path

from tools.utils import haversine_km, save_json_async


class BaseParser(ABC):
    def __init__(self, downloader=None):
        self.downloader = downloader

    @property
    @abstractmethod
    def country(self) -> str:
        """Property that returns the country code e.g. 'FR', 'ES'."""
        pass

    @abstractmethod
    async def parse(self, raw_data):
        """Abstract method to parse raw data."""
        pass

    def format_camera(
        self,
        camera_id: str | int,
        camera_km_point: float,
        camera_view: str,
        camera_type: str,
        coord_x: float | None,
        coord_y: float | None,
        **kwargs,
    ):
        """Standardizes the output dictionary for a camera."""
        base_cam = {
            "camera_id": str(camera_id) if camera_id is not None else "",
            "camera_km_point": float(camera_km_point)
            if camera_km_point is not None
            else 0.0,
            "camera_view": camera_view,
            "camera_type": camera_type,
            "coords": {"X": coord_x, "Y": coord_y},
        }
        base_cam.update(kwargs)
        return base_cam

    def format_highway_output(self, grouped_highways: dict):
        """Converts the internal grouping dictionary to the final list format."""
        return [
            {"highway": {"name": name, "country": self.country, "cameras": cameras}}
            for name, cameras in sorted(grouped_highways.items())
        ]

    def merge_camera_data(
        self,
        *datasets: list[dict],
        match_by: str = "coordinates",
        threshold: float = 0.1,
        check_id: bool = False,
        check_url: bool = False,
    ) -> list[dict]:
        """
        Merges one or more datasets of highway cameras, removing duplicates per highway.
        Datasets should be ordered by priority (highest priority first).
        """
        if match_by not in {"coordinates", "km_point"}:
            raise ValueError("match_by must be 'coordinates' or 'km_point'")

        def _coords(cam: dict) -> tuple[float, float] | None:
            c = cam.get("coords") or {}
            x, y = c.get("X"), c.get("Y")
            if x is not None and y is not None:
                return (x, y)
            return None

        def _spatial_match(cam1: dict, cam2: dict) -> bool:
            if match_by == "coordinates":
                p1, p2 = _coords(cam1), _coords(cam2)
                if p1 is None or p2 is None:
                    return False
                return haversine_km(p1[1], p1[0], p2[1], p2[0]) <= threshold
            km1 = cam1.get("camera_km_point")
            km2 = cam2.get("camera_km_point")
            if (km1 is not None and km2 is not None) and (abs(km1 - km2) <= threshold):
                return True
            return False

        def _is_duplicate(cam: dict, existing: dict) -> bool:
            """Same-ID duplicate: both coords missing OR spatially close (coordinates mode only)."""
            if _coords(cam) is None and _coords(existing) is None:
                return True
            return _spatial_match(cam, existing) if match_by == "coordinates" else False

        # name -> list of cameras
        merged: dict[str, list[dict]] = {}
        countries: dict[str, str] = {}

        for dataset in datasets:
            if not dataset:
                continue
            for entry in dataset:
                highway = entry.get("highway", {})
                name = highway.get("name")
                if not name:
                    continue

                countries.setdefault(name, highway.get("country", self.country))
                target = merged.setdefault(name, [])
                seen_urls = (
                    {c.get("url") for c in target if c.get("url")}
                    if check_url
                    else set()
                )
                cameras_by_id = (
                    {c["camera_id"]: c for c in target if c.get("camera_id")}
                    if check_id
                    else {}
                )

                for cam_in in highway.get("cameras", []):
                    cam = copy.deepcopy(cam_in)
                    url = cam.get("url")
                    cam_id = cam.get("camera_id")

                    # URL dedup
                    if check_url and url and url in seen_urls:
                        continue

                    # ID-based dedup
                    if check_id and cam_id:
                        existing = cameras_by_id.get(cam_id)
                        if existing:
                            if _is_duplicate(cam, existing):
                                continue
                            # Rename to avoid ID collision
                            i = 1
                            new_id = f"{cam_id}_dup{i}"
                            while new_id in cameras_by_id:
                                i += 1
                                new_id = f"{cam_id}_dup{i}"
                            cam["camera_id"] = new_id
                            cam_id = new_id

                    # Spatial dedup (only when not using ID-based checks)
                    if not check_id and any(_spatial_match(cam, c) for c in target):
                        continue

                    target.append(cam)
                    if check_url and url:
                        seen_urls.add(url)
                    if cam_id:
                        cameras_by_id[cam_id] = cam

        return [
            {
                "highway": {
                    "name": name,
                    "country": countries[name],
                    "cameras": sorted(
                        cams, key=lambda c: c.get("camera_km_point", 0.0)
                    ),
                }
            }
            for name, cams in sorted(merged.items())
        ]

    async def get_parsed_data(self, output_file=None, output_folder=None):
        """Orchestrates downloading and parsing data."""
        raw_data = None
        if self.downloader:
            raw_data = await self.downloader.get_data()

        # Handle async/sync parse method
        if inspect.iscoroutinefunction(self.parse):
            parsed_data = await self.parse(raw_data)
        else:
            parsed_data = self.parse(raw_data)

        if output_file:
            await save_json_async(parsed_data, output_file)
        elif output_folder:
            file_name = f"cameras_{self.country.lower()}{'_gov' if self.country in ['ES', 'UK'] else ''}.json"  # FR handles saving independently
            await save_json_async(parsed_data, Path(output_folder) / file_name)

        return parsed_data
