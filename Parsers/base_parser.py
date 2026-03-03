import copy
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from tools.utils import haversine_km, save_json


class BaseParser(ABC):
    """
    Abstract base class for all highway camera parsers.

    This class provides common functionality for standardizing camera data format,
    merging datasets, deduplicating cameras based on spatial coordinates or kilometer
    points, and orchestrating the download and parsing steps.
    """

    def __init__(self, downloader: Any = None) -> None:
        """
        Initializes the BaseParser.

        Args:
            downloader (Any, optional): The downloader instance responsible for
                fetching raw camera data. Defaults to None.
        """
        self.downloader = downloader

    @property
    @abstractmethod
    def country(self) -> str:
        """
        Property that returns the country code.

        Returns:
            str: The two-letter country code (e.g., 'FR', 'ES').
        """
        pass

    @abstractmethod
    async def parse(self, raw_data: Any) -> Any:
        """
        Abstract method to parse raw data.

        Args:
            raw_data (Any): The raw data fetched by the downloader.

        Returns:
            Any: The parsed camera data.
        """
        pass

    @staticmethod
    def format_camera(
        camera_id: str | int | None,
        camera_km_point: float | None,
        camera_view: str,
        camera_type: str,
        coord_x: float | None,
        coord_y: float | None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Standardizes the output dictionary for a camera.

        Args:
            camera_id (str | int | None): The unique identifier for the camera.
            camera_km_point (float | None): The kilometer point location of the camera, if available
            camera_view (str): The viewing direction or angle of the camera, if available
            camera_type (str): The type or format of the camera feed (e.g., 'img', 'vid').
            coord_x (float | None): The longitude (X) coordinate.
            coord_y (float | None): The latitude (Y) coordinate.
            **kwargs (Any): Additional properties to add to the camera dictionary.

        Returns:
            dict[str, Any]: A standardized dictionary representing the camera.
        """
        base_cam: dict[str, Any] = {
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

    def format_highway_output(
        self, grouped_highways: dict[str, list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """
        Create the highway format from internal data structure.

        Args:
            grouped_highways (dict[str, list[dict[str, Any]]]): A dictionary mapping highway
                names to lists of camera dictionaries.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each representing a highway and its cameras.
        """
        return [
            {"highway": {"name": name, "country": self.country, "cameras": cameras}}
            for name, cameras in sorted(grouped_highways.items())
        ]

    def merge_camera_data(
        self,
        *datasets: list[dict[str, Any]],
        match_by: str = "coordinates",
        threshold: float = 0.1,
        check_id: bool = False,
        check_url: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Merges one or more datasets of highway cameras, removing duplicates per highway.
        Datasets should be ordered by priority (highest priority first).

        Args:
            *datasets (list[dict[str, Any]]): Variable number of datasets to merge.
            match_by (str, optional): The method to match duplicates, either "coordinates"
                or "km_point". Defaults to "coordinates".
            threshold (float, optional): The threshold distance for spatial matching (in km)
                or km point matching. Defaults to 0.1.
            check_id (bool, optional): Whether to check for duplicate camera IDs. Defaults to False.
            check_url (bool, optional): Whether to deduplicate by exact URL matching. Defaults to False.

        Raises:
            ValueError: If `match_by` is not 'coordinates' or 'km_point'.

        Returns:
            list[dict[str, Any]]: The merged and deduplicated list of highway dictionary entries.
        """
        if match_by not in {"coordinates", "km_point"}:
            raise ValueError("match_by must be 'coordinates' or 'km_point'")

        def _coords(cam: dict[str, Any]) -> tuple[float, float] | None:
            """Extracts (X, Y) coordinates from a camera."""
            c = cam.get("coords") or {}
            x, y = c.get("X"), c.get("Y")
            if x is not None and y is not None:
                return (x, y)
            return None

        def _spatial_match(cam1: dict[str, Any], cam2: dict[str, Any]) -> bool:
            """Checks if two cameras are geographically close or have a similar km point."""
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

        def _is_duplicate(cam: dict[str, Any], existing: dict[str, Any]) -> bool:
            """Same-ID duplicate: both coords missing OR spatially close (coordinates mode only)."""
            if _coords(cam) is None and _coords(existing) is None:
                return True
            return _spatial_match(cam, existing) if match_by == "coordinates" else False

        # name -> list of cameras
        merged: dict[str, list[dict[str, Any]]] = {}
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

    async def get_parsed_data(
        self,
        output_file: str | Path | None = None,
        output_folder: str | Path | None = None,
    ) -> Any:
        """
        Orchestrates downloading and parsing data.

        Args:
            output_file (str | Path | None, optional): Specific file path to save output. Defaults to None.
            output_folder (str | Path | None, optional): Folder to save output according to country format. Defaults to None.

        Returns:
            Any: The parsed camera data.
        """
        raw_data = None
        if self.downloader:
            raw_data = await self.downloader.get_data()

        # Handle async/sync parse method
        if inspect.iscoroutinefunction(self.parse):
            parsed_data = await self.parse(raw_data)
        else:
            parsed_data = self.parse(raw_data)

        if output_file:
            save_json(parsed_data, output_file)
        elif output_folder:
            file_name = f"cameras_{self.country.lower()}{'_gov' if self.country in ['ES', 'UK'] else ''}.json"  # France(FR) and Italy(IT) handle saving independently
            save_json(parsed_data, Path(output_folder) / file_name)

        return parsed_data
