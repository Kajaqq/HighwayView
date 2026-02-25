import argparse
import json
from pathlib import Path
from natsort import natsorted

from tools.utils import create_url, load_json, get_country
from config import CONSTANTS

COUNTRY_MAP = CONSTANTS.COMMON.COUNTRY_MAP
HTML_DIR = Path(CONSTANTS.COMMON.HTML_DIR)
DEFAULT_INTERVAL = CONSTANTS.COMMON.SLIDESHOW_INTERVAL


def get_camera_urls(
    json_data: list[dict],
    camera_ids: list[str] | None = None,
    highways: list[str] | None = None,
    apply_sort: bool = False,
) -> tuple[list[tuple[str, str, str, int, str]], str]:

    country = get_country(json_data)
    cameras = []

    if camera_ids:
        camera_map = {}
        for highway_item in json_data:
            hw_name = highway_item["highway"]["name"]
            for cam in highway_item["highway"]["cameras"]:
                camera_map[cam["camera_id"]] = (cam, hw_name)

        camera_number = 1

        for cid in camera_ids:
            if cid in camera_map:
                camera, highway_name = camera_map[cid]

                if country == "IT":
                    url = camera["url"]
                    media_type = "video"
                else:
                    camera_type = camera.get("camera_type", "")
                    url, _ = create_url(country, cid, camera_type)
                    media_type = (
                        "video" if camera_type in ["vid", "asfa_vid"] else "image"
                    )

                cameras.append((cid, url, highway_name, camera_number, media_type))

                camera_number += 1
            else:
                print(f"Warning: Camera ID {cid} not found in JSON data.")

        return cameras, country

    if apply_sort:
        json_data = natsorted(json_data, key=lambda x: x["highway"]["name"])

    for highway_item in json_data:
        highway = highway_item["highway"]
        highway_name = highway["name"]

        if highways and highway_name not in highways:
            continue

        camera_number = 0
        for camera in highway["cameras"]:
            camera_id = camera["camera_id"]
            camera_number += 1

            if country == "IT":
                url = camera["url"]
                media_type = "video"
            else:
                camera_type = camera.get("camera_type", "")
                url, _ = create_url(country, camera_id, camera_type)
                media_type = "video" if camera_type in ["vid", "asfa_vid"] else "image"

            cameras.append((camera_id, url, highway_name, camera_number, media_type))

    return cameras, country


# language=html
def generate_html(
    cameras: list[tuple[str, str, str, int, str]], interval: int, country: str
) -> str:
    """
    Generate an optimized HTML slideshow for OBS with lazy loading and memory management.

    Args:
        cameras: List of tuples (camera_id, camera_url, highway_name, camera_number, media_type)
        interval: Time in seconds between transitions
        country: Country code (e.g., "FR", "ES", "IT")

    Returns:
        HTML string
    """

    camera_data = [
        {"id": cid, "url": url, "highway": hw, "number": num, "type": media_type}
        for cid, url, hw, num, media_type in cameras
    ]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Camera Slideshow</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            background: transparent;
            overflow: hidden;
            font-family: Arial, sans-serif;
        }}

        .slideshow-container {{
            position: relative;
            width: 100vw;
            height: 100vh;
            overflow: hidden;
        }}

        .slide {{
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            opacity: 0;
            transition: opacity 1s ease-in-out;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        }}

        .slide.active {{
            opacity: 1;
        }}

        .slide img,
        .slide video {{
            max-width: 100%;
            max-height: 100%;
            object-fit: contain;
        }}

        .slide-info {{
            position: absolute;
            bottom: 20px;
            left: 20px;
            background: rgba(0, 0, 0, 0.7);
            color: white;
            padding: 10px 20px;
            border-radius: 5px;
            font-size: 18px;
        }}

        .error-screen {{
            width: 100%;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            background: linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%);
            color: #ffffff;
            text-align: center;
            padding: 40px;
        }}

        .error-icon {{
            font-size: 80px;
            margin-bottom: 20px;
            color: #ff6b6b;
        }}

        .error-title {{
            font-size: 32px;
            font-weight: bold;
            margin-bottom: 10px;
            color: #ff6b6b;
        }}

        .error-message {{
            font-size: 20px;
            color: #cccccc;
            margin-bottom: 15px;
        }}

        .error-details {{
            font-size: 16px;
            color: #999999;
        }}
    </style>
</head>
<body>
    <div class="slideshow-container" id="slideshow"></div>

    <script>
        const cameras = {json.dumps(camera_data)};
        const interval = {interval * 1000}; // Convert to milliseconds
        const country = {json.dumps(country)};
        const PRELOAD_COUNT = 2; // Number of slides to preload ahead
        const RETRY_DELAY = 300000; // 5 minutes retry delay

        let currentIndex = 0;
        let isTransitioning = false;
        const container = document.getElementById('slideshow');

            // Active slides cache (only keep 3 in DOM: current + 2 preloaded)
            const activeSlides = new Map();

            // Pause slideshow when tab is hidden
            let isPaused = false;
            document.addEventListener('visibilitychange', () => {{
                isPaused = document.hidden;
                if (!isPaused) {{
                    // Resume and refresh current slide
                    refreshCurrentSlide();
                }}
            }});

        // Create error screen element
        function createErrorScreen(camera) {{
            const errorScreen = document.createElement('div');
            errorScreen.className = 'error-screen';

            const errorIcon = document.createElement('div');
            errorIcon.className = 'error-icon';
            errorIcon.textContent = '⚠';

            const errorTitle = document.createElement('div');
            errorTitle.className = 'error-title';
            errorTitle.textContent = 'Camera Offline';

            const errorMessage = document.createElement('div');
            errorMessage.className = 'error-message';
            errorMessage.textContent = `${{camera.highway}} - Camera ${{camera.number || camera.id}}`;

            const errorDetails = document.createElement('div');
            errorDetails.className = 'error-details';
            errorDetails.textContent = 'Unable to load camera feed';

            errorScreen.appendChild(errorIcon);
            errorScreen.appendChild(errorTitle);
            errorScreen.appendChild(errorMessage);
            errorScreen.appendChild(errorDetails);

            return errorScreen;
        }}

        // Clean up media element to prevent memory leaks
        function cleanupMedia(mediaElement) {{
            if (!mediaElement) return;

            if (mediaElement.tagName === 'VIDEO') {{
                mediaElement.pause();
                mediaElement.removeAttribute('src');
                mediaElement.load(); // This releases the video resource
            }} else if (mediaElement.tagName === 'IMG') {{
                mediaElement.removeAttribute('src');
            }}
        }}

            // Clean up slide and remove from DOM
            function destroySlide(index) {{
                const slide = activeSlides.get(index);
                if (!slide) return;

                const mediaElement = slide.querySelector('img, video');
                cleanupMedia(mediaElement);

                slide.remove();
                activeSlides.delete(index);
            }}

            // Create slide element
            function createSlide(index, isActive = false) {{
                // Don't recreate if already exists
                if (activeSlides.has(index)) {{
                    return activeSlides.get(index);
                }}

                const camera = cameras[index];
                const slide = document.createElement('div');
                slide.className = 'slide';
                slide.dataset.index = index;

                if (isActive) {{
                    slide.classList.add('active');
                }}

                // Create media element
                let mediaElement;
                if (camera.type === 'video') {{
                    mediaElement = document.createElement('video');
                    mediaElement.muted = true;
                    mediaElement.autoplay = isActive;
                    mediaElement.loop = true;
                    mediaElement.playsInline = true; // Important for mobile/OBS
                    mediaElement.preload = 'auto';

                    mediaElement.onerror = function(e) {{
                        console.error(`Video load error for ${{camera.id}}:`, e);
                        camera.failed = true;
                        camera.lastFailed = Date.now();
                        
                        slide.innerHTML = '';
                        slide.appendChild(createErrorScreen(camera));
                        
                        // If error happens on active slide, skip quickly
                        if (slide.classList.contains('active') && !isTransitioning) {{
                            setTimeout(nextSlide, 2000);
                        }}
                    }};

                    mediaElement.onloadeddata = function() {{
                        if (isActive || slide.classList.contains('active')) {{
                            mediaElement.play().catch(e => console.error('Play error:', e));
                        }}
                    }};
                }} else {{
                    mediaElement = document.createElement('img');
                    mediaElement.alt = `Camera ${{camera.id}}`;
                    mediaElement.loading = 'eager'; // Force immediate loading for preload

                    mediaElement.onerror = function(e) {{
                        console.error(`Image load error for ${{camera.id}}:`, e);
                        slide.innerHTML = '';
                        slide.appendChild(createErrorScreen(camera));
                    }};
                }}

                slide.appendChild(mediaElement);

                // Add info overlay for France
                if (country === 'FR') {{
                    const info = document.createElement('div');
                    info.className = 'slide-info';
                    info.textContent = `Highway: ${{camera.highway}} - Camera ${{camera.number}}`;
                    slide.appendChild(info);
                }}

                container.appendChild(slide);
                activeSlides.set(index, slide);

                // Load the media
                loadMedia(index, mediaElement, camera);

                return slide;
            }}

        // Load media with cache-busting for images
        function loadMedia(index, mediaElement, camera) {{
            if (camera.type === 'video') {{
                mediaElement.src = camera.url;
            }} else {{
                // Add cache-busting for images
                const cacheBuster = '?t=' + Date.now();
                mediaElement.src = camera.url + cacheBuster;
            }}
        }}

        // Refresh current slide media
        function refreshCurrentSlide() {{
            const slide = activeSlides.get(currentIndex);
            if (!slide) return;

            // Skip if error screen is showing
            if (slide.querySelector('.error-screen')) return;

            const camera = cameras[currentIndex];
            const mediaElement = slide.querySelector('img, video');

            if (camera.type === 'image' && mediaElement) {{
                // Reload image with fresh cache-buster
                const cacheBuster = '?t=' + Date.now();
                mediaElement.src = camera.url + cacheBuster;
            }} else if (camera.type === 'video' && mediaElement) {{
                // Ensure video is playing
                if (mediaElement.paused) {{
                    mediaElement.play().catch(e => console.error('Play error:', e));
                }}
            }}
        }}

        // Preload upcoming slides
        function preloadSlides() {{
            for (let i = 1; i <= PRELOAD_COUNT; i++) {{
                const nextIndex = (currentIndex + i) % cameras.length;
                createSlide(nextIndex, false);
            }}
        }}

        // Clean up old slides (keep only current + PRELOAD_COUNT)
        function cleanupOldSlides() {{
            const keepIndices = new Set();
            keepIndices.add(currentIndex);

            for (let i = 1; i <= PRELOAD_COUNT; i++) {{
                keepIndices.add((currentIndex + i) % cameras.length);
            }}

            // Remove slides that are not in the keep list
            for (const [index, slide] of activeSlides.entries()) {{
                if (!keepIndices.has(index)) {{
                    destroySlide(index);
                }}
            }}
        }}

        function nextSlide() {{
                if (isPaused || isTransitioning) return;

                // Find next viable camera (skip failed ones)
                let attempts = 0;
                let nextIndex = (currentIndex + 1) % cameras.length;
                
                while (attempts < cameras.length) {{
                    const cam = cameras[nextIndex];
                    if (cam.failed) {{
                        // Check if we should retry
                        const timeSinceFail = Date.now() - (cam.lastFailed || 0);
                        if (timeSinceFail < RETRY_DELAY) {{
                            // Skip this camera
                            nextIndex = (nextIndex + 1) % cameras.length;
                            attempts++;
                            continue;
                        }}
                    }}
                    break;
                }}

                isTransitioning = true;

            // Get current slide
            const currentSlide = activeSlides.get(currentIndex);
            if (currentSlide) {{
                currentSlide.classList.remove('active');

                // Pause video if leaving
                const currentVideo = currentSlide.querySelector('video');
                if (currentVideo) {{
                    currentVideo.pause();
                }}
            }}

            // Move to next
            currentIndex = nextIndex;

            // Ensure next slide exists
            const nextSlideEl = createSlide(currentIndex, true);
            nextSlideEl.classList.add('active');

            // Start video if it's a video slide
            const nextVideo = nextSlideEl.querySelector('video');
            if (nextVideo && !nextSlideEl.querySelector('.error-screen')) {{
                nextVideo.play().catch(e => console.error('Play error:', e));
                }}

            // Preload upcoming slides
            preloadSlides();

            // Clean up old slides after transition completes
            setTimeout(() => {{
                cleanupOldSlides();
                isTransitioning = false;
            }}, 1100); // Wait for opacity transition to complete (1s + 100ms buffer)
        }}

            // Initialize slideshow
            function init() {{
                // Create initial slide
                createSlide(currentIndex, true);

                // Preload next slides
                preloadSlides();

                // Start slideshow
                const slideshowInterval = setInterval(() => {{
                    if (!isPaused) {{
                        nextSlide();
                    }}
                }}, interval);
            }}

            // Start when DOM is ready
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', init);
            }} else {{
                init();
            }}
    </script>
</body>
</html>"""

    return html


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate an HTML slideshow from camera JSON data for OBS"
    )
    parser.add_argument(
        "json_file",
        type=str,
        help="Path to the camera JSON file (e.g., cameras_es_online.json)",
    )
    parser.add_argument(
        "-f",
        "--output_file",
        type=str,
        help="Output HTML file name",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default=HTML_DIR,
        help="Output HTML file directory",
    )
    parser.add_argument(
        "-c",
        "--camera-ids",
        type=str,
        nargs="+",
        help="Specific camera IDs to include (space-separated)",
    )
    parser.add_argument(
        "-hw",
        "--highways",
        type=str,
        help="Highway names to include (comma-separated, e.g., A-1,A-2,A-6)",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Interval time in seconds between camera transitions (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "-un",
        "--include_unknown",
        action="store_true",
        help="Whether to include unknown cameras (default: false)",
    )
    parser.add_argument(
        "--sort",
        action="store_true",
        help="Sort the cameras naturally",
    )
    arguments = parser.parse_args()

    if arguments.camera_ids and arguments.highways:
        parser.error("Cannot specify both --camera-ids and --highways")

    return arguments


def main(args):
    # Load JSON data
    try:
        json_data = load_json(args.json_file)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return

    # Parse highways from comma-separated string
    highways_list = None
    if args.highways:
        highways_list = [hw.strip() for hw in args.highways.split(",")]

    # Get camera URLs
    apply_sort = args.sort
    cameras, country = get_camera_urls(
        json_data, args.camera_ids, highways_list, apply_sort
    )

    if not cameras:
        print("No cameras found matching the specified criteria")
        return

    # Generate HTML
    html_content = generate_html(cameras, args.interval, country)

    # Save HTML file
    if not args.output_file:
        try:
            args.output_file = f"Cameras_{COUNTRY_MAP[country]}.html"
        except KeyError:
            print(f"Warning: Unknown country code '{country}', using default filename")
            args.output_file = "Cameras.html"

    output_path = Path(f"{args.output_dir}\\{args.output_file}")
    try:
        if not output_path.parent.exists():
            Path(output_path.parent).mkdir(parents=True, exist_ok=True)
        output_path.write_text(html_content, encoding="utf-8")
        print(f"HTML slideshow created: {output_path}")
    except Exception as e:
        print(f"Error writing HTML file: {e}")


if __name__ == "__main__":
    parsed_args = parse_args()
    main(parsed_args)
