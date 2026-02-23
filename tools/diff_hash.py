import collections
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import dhash
import pybktree
import ffmpeg
import ffmpeg.filters
from PIL import Image
from config import CONSTANTS

SEP = CONSTANTS.COMMON.SEPARATOR
IMAGE_EXTENSIONS = CONSTANTS.COMMON.IMAGE_EXTENSIONS
VIDEO_EXTENSIONS = CONSTANTS.COMMON.VIDEO_EXTENSIONS

Camera = collections.namedtuple("Camera", "bits id")


def get_video_frame(video_file: Path):
    output_path = video_file.with_suffix(".png")
    input_file = ffmpeg.input(video_file)
    scaled = input_file.scale(w=352, h=288)
    ffmpeg.output(scaled, filename=output_path, vframes=1).run(
        overwrite_output=True, quiet=True
    )
    video_file.unlink()


def get_image_hash(img_file):
    """Opens an image and returns its dhash bit integer."""
    try:
        img_file = Path(img_file)
        with Image.open(img_file) as img:
            h_bits = dhash.dhash_int(img, size=8)
            return Camera(h_bits, img_file.stem)

    except Exception as e:
        print(f"Error processing {img_file}: {e}")
        return None


def item_distance(x, y):
    """Calculates the Hamming distance between two hash bit integers."""
    return dhash.get_num_bits_different(x.bits, y.bits)


def get_duplicates(tree, hash_list):
    dupes = set()
    for cam in hash_list:
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


def main(file_path=None):
    print(f"Hashing images in {file_path}...")

    video_files = [
        f for f in file_path.iterdir() if f.suffix.lower() in VIDEO_EXTENSIONS
    ]

    if video_files:
        with ProcessPoolExecutor() as executor:
            list(executor.map(get_video_frame, video_files))

    image_files = [
        f for f in file_path.iterdir() if f.suffix.lower() in IMAGE_EXTENSIONS
    ]

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(get_image_hash, image_files))

    hash_list = [r for r in results if r is not None]

    if not hash_list:
        print("No files processed.")
        return None

    tree = pybktree.BKTree(item_distance, hash_list)

    print(SEP)
    print("Searching for duplicates...")

    duplicate_ids = get_duplicates(tree, hash_list)

    return duplicate_ids


def cleanup_folder(folder_path: Path):
    for file in folder_path.iterdir():
        file.unlink()


def folder_hash(folder_path):
    duplicates = main(Path(folder_path))
    cleanup_folder(folder_path)
    return duplicates


if __name__ == "__main__":
    folder = Path("data/test")
    print(main(folder))
