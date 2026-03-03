import datetime
import json
import math
from itertools import cycle
from pathlib import Path
from typing import Any

from lambert import Lambert93, convertToWGS84Deg

from config import CONSTANTS

EARTH_RADIUS_KM: float = CONSTANTS.COMMON.EARTH_RADIUS_KM
DEFAULT_HEADERS: dict[str, str] = CONSTANTS.COMMON.DEFAULT_HEADERS


def check_parent_dir(path: Path) -> None:
    """
    Ensures that the parent directory of a given path exists.

    Args:
        path (Path): The file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

def check_json(json_data: Any, indent: int | None) -> str:
    """
    Validates and formats JSON data to a string.

    Args:
        json_data (Any): The data to serialize.
        indent (int | None): The indentation level for pretty-printing.

    Raises:
        ValueError: If the data cannot be serialized to JSON.

    Returns:
        str: The JSON formatted string.
    """
    if isinstance(json_data, str):
        return json_data
    try:
        return json.dumps(json_data, ensure_ascii=False, indent=indent)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not serializable to JSON: {e}") from e

def save_json(json_data: Any, output: Path | str) -> None:
    """
    Synchronously saves JSON data to a file with proper error handling.

    Args:
        json_data (Any): The data to save.
        output (Path | str): The output file path.

    Raises:
        OSError: If the file cannot be written.
    """
    output_path = Path(output)
    check_parent_dir(output_path)
    try:
        content: str = check_json(json_data, indent=4)
        output_path.write_text(content, encoding="utf-8")
    except OSError as e:
        raise OSError(f"Failed to write file {output_path}: {e}") from e


def load_json(json_data: Path | str | list[Any] | dict[str, Any]) -> Any:
    """
    Loads JSON data from a file, raw string, or passes it through if already dict/list.

    Args:
        json_data (Path | str | list | dict): The source data.

    Raises:
        OSError: If reading the file fails.
        ValueError: If JSON parsing fails.

    Returns:
        Any: The parsed JSON data.
    """
    if isinstance(json_data, (list, dict)):
        return json_data
    try:
        if isinstance(json_data, Path):
            with Path.open(json_data, encoding="utf-8") as infile:
                return json.load(infile)
        else:
            return json.loads(json_data)
    except OSError as e:
        raise OSError(f"Failed to read file {json_data}: {e}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON data: {e}") from e


def create_url(
    base: str, camera_id: str | int, camera_type: str
) -> tuple[str, str | None]:
    """
    Constructs the full media URL based on the country, ID, and type.

    Args:
        base (str): The country code (e.g., 'FR', 'ES', 'UK').
        camera_id (str | int): The camera identifier.
        camera_type (str): The type of camera ('vid', 'img', 'asfa_vid').

    Raises:
        ValueError: If an invalid country or type is provided.

    Returns:
        tuple[str, str | None]: A tuple consisting of the full URL and the file extension.
    """
    if base == "FR":
        if camera_type == "asfa_vid":
            base_url: str = CONSTANTS.FRANCE.ASFA.CAMERA_URL
            ext: str = CONSTANTS.FRANCE.ASFA.VIDEO_EXT
            return base_url.format(camera_id=camera_id), ext
        base_url = CONSTANTS.FRANCE.CAMERA_URL
        ext_dict = {
            "vid": CONSTANTS.FRANCE.VIDEO_EXT,
            "img": CONSTANTS.FRANCE.IMAGE_EXT,
        }
        ext2 = ext_dict.get(camera_type)
        return f"{base_url}{camera_id}{ext2}", ext2
    if base == "ES":
        base_url = CONSTANTS.SPAIN.CAMERA_URL
        ext2 = CONSTANTS.SPAIN.IMAGE_EXT
        return f"{base_url}{camera_id}{ext2}", ext2
    if base == "UK":
        base_url = CONSTANTS.UK.CAMERA_URL
        ext2 = CONSTANTS.UK.IMAGE_EXT
        return f"{base_url}{camera_id}{ext2}", ext2
    else:
        raise ValueError("Invalid data")


def unix_to_datetime(
    timestamp: int | float | str, tz: datetime.tzinfo = CONSTANTS.FRANCE.PARIS_TZ
) -> str:
    """
    Converts a Unix timestamp to a formatted datetime string with timezone normalization.

    Args:
        timestamp (int | float | str): The Unix timestamp, in seconds or milliseconds.
        tz (datetime.tzinfo, optional): Timezone info. Defaults to CONSTANTS.FRANCE.PARIS_TZ.

    Returns:
        str: The formatted datetime string (YYYYMMDD-HHMMSS).
    """
    timestamp_len = len(str(timestamp))
    if timestamp_len > 10:
        zeros_to_remove = timestamp_len - 10
        divisor = 10**zeros_to_remove
        normalized_timestamp = float(timestamp) / divisor
    else:
        normalized_timestamp = float(timestamp)
    dt = datetime.datetime.fromtimestamp(normalized_timestamp, tz)
    return dt.strftime("%Y%m%d-%H%M%S")


def xor_decode(msg: bytes, key_str: str) -> str:
    """
    XOR decodes a message with a given key.

    Args:
        msg (bytes): The XOR encoded message.
        key_str (str): The XOR symmetric key string.

    Returns:
        str: The decoded string.
    """
    key = key_str.encode("utf-8")
    decoded = bytearray(b ^ k for b, k in zip(msg, cycle(key)))
    return decoded.decode("utf-8")


def convert_to_wgs84(lon: float, lat: float) -> tuple[float, float]:
    """
    Converts Lambert-93 coordinates to WGS-84 coordinates.

    Args:
        lon (float): Lambert-93 X coordinate.
        lat (float): Lambert-93 Y coordinate.

    Returns:
        tuple[float, float]: Extracted (Longitude, Latitude) tuple in WGS-84.
    """
    pt = convertToWGS84Deg(lon, lat, Lambert93)
    return pt.getX(), pt.getY()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculates the great-circle distance between two points on Earth in kilometers.

    Args:
        lat1 (float): Latitude of the first point.
        lon1 (float): Longitude of the first point.
        lat2 (float): Latitude of the second point.
        lon2 (float): Longitude of the second point.

    Returns:
        float: The distance between the points in kilometers.
    """
    r: float = EARTH_RADIUS_KM
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    # Clamp value to 1.0 to handle floating-point errors (prevents ValueError in asin)
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def get_country(camera_data: list[dict[str, Any]]) -> str:
    """
    Extracts the country identifier from the parsed camera data structure.

    Args:
        camera_data (list[dict[str, Any]]): The parsed camera list grouped by highway.

    Returns:
        str: The country code.
    """
    return camera_data[0]["highway"]["country"]
