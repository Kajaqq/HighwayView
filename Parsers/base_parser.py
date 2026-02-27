import inspect
from abc import ABC, abstractmethod
from pathlib import Path

from tools.utils import save_json_async


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
            file_name = f"cameras_{self.country.lower()}{'_gov' if self.country in ['ES', 'UK'] else ''}.json"
            if self.country == "FR":
                file_name = "cameras_fr_merged.json"  # Special case or default behaviour handled per parser if overriding
            await save_json_async(parsed_data, Path(output_folder) / file_name)

        return parsed_data
