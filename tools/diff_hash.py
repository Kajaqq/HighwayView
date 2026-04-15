from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import NamedTuple

import dhash
import pybktree
import ffmpeg
import ffmpeg.filters
from PIL import Image
from config import CONSTANTS

SEP: str = CONSTANTS.COMMON.SEPARATOR
IMAGE_EXTENSIONS: tuple[str] = CONSTANTS.COMMON.IMAGE_EXTENSIONS
VIDEO_EXTENSIONS: tuple[str] = CONSTANTS.COMMON.VIDEO_EXTENSIONS


class Camera(NamedTuple):
    bits: int
    id: str


def get_video_frame(video_file: Path) -> None:
    """
    Extracts the first frame of a video file and saves it as a PNG, then deletes the video.

    Args:
        video_file (Path): Path to the video file.
    """
    output_path = video_file.with_suffix(".png")
    input_file = ffmpeg.input(str(video_file), ss="00:00:00")
    # scale all images to the same size
    # save the first frame as a PNG
    ffmpeg.output(input_file, filename=str(output_path), vframes=1, vf='scale=352:288').run(
        overwrite_output=True, quiet=True
    )
    # remove the video
    video_file.unlink()


def get_image_hash(img_file: Path | str) -> Camera | None:
    """
    Opens an image and returns its dhash bit integer wrapped in a Camera namedtuple.

    Args:
        img_file (Path | str): Path to the image file.

    Returns:
        Camera | None: The computed hash and ID, or None if processing fails.
    """
    try:
        img_file_path = Path(img_file)
        with Image.open(img_file_path) as img:
            # Hash the image
            h_bits = dhash.dhash_int(img, size=8)
            return Camera(h_bits, img_file_path.stem)

    except Exception as e:
        print(f"Error processing {img_file}: {e}")
        return None


def item_distance(x: Camera, y: Camera) -> int:
    """
    Calculates the Hamming distance between two hash bit integers.

    Args:
        x (Camera): The first camera hash.
        y (Camera): The second camera hash.

    Returns:
        int: The Hamming distance.
    """
    return dhash.get_num_bits_different(x.bits, y.bits)


def get_duplicates(tree: pybktree.BKTree, hash_list: list[Camera]) -> set[str]:
    """
    Finds duplicated images within a BK-Tree based on Hamming distance.

    Args:
        tree (pybktree.BKTree): The populated BKTree.
        hash_list (list[Camera]): The list of all camera hashes.

    Returns:
        set[str]: A set of duplicate camera IDs.
    """
    dupes: set[str] = set()
    for cam in hash_list:
        # find duplicates within 4 bits of difference
        matches = tree.find(cam, 8)
        duplicates = [m[1].id for m in matches if m[1].id != cam.id]

        if duplicates and cam.id not in dupes:
            print(f"Camera: {cam.id} | Duplicates found: {', '.join(duplicates)}")
            dupes.add(cam.id)
            for d in duplicates:
                dupes.add(d)
    if not dupes:
        print("No duplicates found.")
    return dupes


def main(file_path: Path | None = None) -> set[str] | None:
    """
    Processes a directory of images/videos, hashes them, and detects duplicates.

    Args:
        file_path (Path | None, optional): The directory containing media files. Defaults to None.

    Returns:
        set[str] | None: A set of duplicate camera IDs, or None if no files processed.
    """
    if not file_path:
        return None

    print(f"Hashing images in {file_path}...")

    video_files: list[Path] = []
    image_files: list[Path] = []
    for f in file_path.iterdir():
        ext = f.suffix.lower()
        if ext in VIDEO_EXTENSIONS:
            video_files.append(f)
        elif ext in IMAGE_EXTENSIONS:
            image_files.append(f)

    if video_files:
        with ThreadPoolExecutor() as thread_executor:
            list(thread_executor.map(get_video_frame, video_files))

    results: list[Camera | None] = []
    if image_files or video_files:
        with ProcessPoolExecutor() as process_executor:
            results = list(
                process_executor.map(get_image_hash, image_files, chunksize=100)
            )

    hash_list: list[Camera] = [r for r in results if r is not None]

    if not hash_list:
        print("No files processed.")
        return None

    tree = pybktree.BKTree(item_distance, hash_list)

    print(SEP)
    print("Searching for duplicates...")

    duplicate_ids: set[str] = get_duplicates(tree, hash_list)

    return duplicate_ids


def cleanup_folder(folder_path: Path) -> None:
    """
    Deletes all files in a folder.

    Args:
        folder_path (Path): The folder to empty.
    """
    for file in folder_path.iterdir():
        if file.is_file():
            file.unlink()


def folder_hash(folder_path: Path | str) -> set[str] | None:
    """
    Main orchestrator function for folder hashing.

    Args:
        folder_path (Path | str): Path to the folder.

    Returns:
        set[str] | None: Set of duplicate IDs.
    """
    f_path = Path(folder_path)
    duplicates = main(f_path)
    cleanup_folder(f_path)
    return duplicates


if __name__ == "__main__":
    folder = Path("data/test")
    print(main(folder))
