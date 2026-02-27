from pathlib import Path
from zoneinfo import ZoneInfo


class CONSTANTS:
    class COMMON:
        PROJECT_ROOT = Path(__file__).parent
        HTTPS_PREFIX = "https:"
        SEPARATOR = "=" * 36
        VIDEO_EXTENSIONS = (".mp4", ".flv")
        IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")
        RATE_LIMIT = 50
        HTTP_TIMEOUT = 20.00
        SLIDESHOW_INTERVAL = 7
        EARTH_RADIUS_KM = 6371.0
        COUNTRY_MAP = {"ES": "Spain", "FR": "France", "IT": "Italy", "UK": "UK"}
        DATA_DIR = PROJECT_ROOT / Path("data/")
        IMG_DIR_NAME = Path("images/")
        IMG_DIR = DATA_DIR / IMG_DIR_NAME
        HTML_DIR = Path('html')
        DEFAULT_HEADERS = {
            "accept": "*/*",
            "content-type": "application/json",
            "priority": "u=1, i",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
        }

    class FRANCE:
        BASE_URL = "https://www.bison-fute.gouv.fr/"
        TIMESTAMP_URL = "data/iteration/date.json"
        CAMERA_API = "data/data-{datetime}/trafic/maintenant/camerasOL6/camerasOL6.json"
        CAMERA_URL = "https://www.bison-fute.gouv.fr/camera-upload/"
        VIDEO_EXT = ".mp4"
        IMAGE_EXT = ".png"
        PARIS_TZ = ZoneInfo("Europe/Paris")
        HIGHWAY_SEQUENCE = [
            # Northern Gateways - UK & Belgium (0:00 - 1:30)
            ("A-16", 8),  # Calais Port/Tunnel & Amiens (Max usage of available 11)
            ("A-1", 3),  # Lille - Paris Link (All 3 available)
            ("A-2", 1),  # Brussels Connector (All 1 available)
            ("A-26", 2),  # Calais - Reims "English Route" (All 2 available)
            ("A-29", 4),  # Le Havre Port / Normandy (All 4 available)
            # Eastern Corridor - Lux & Germany (1:30 - 3:15)
            ("A-31", 9),  # Luxembourg Border/Metz/Nancy (Critical N-S Axis)
            ("A-4", 4),  # Metz - Paris (All 4 available)
            ("A-36", 2),  # Mulhouse/Germany Border (All 2 available)
            # The Spine - Paris to Mediterranean (3:15 - 4:45)
            ("A-6", 7),  # Paris - Lyon "Sun Motorway"
            ("A-7", 5),  # Lyon - Marseille (All 5 available - High Congestion)
            # Alpine Access - Italy & Switzerland (4:45 - 6:00)
            ("A-40", 5),  # Geneva/Mont Blanc Approach
            ("A-43", 5),  # Lyon - Italy/Fréjus Tunnel (All 5 available)
            ("A-48", 2),  # Grenoble Approach
            # Central Massif - The Alternative South (6:00 - 7:15)
            ("A-75", 9),  # Clermont - Béziers (Millau/Winter Snow Risk)
            ("A-71", 2),  # Orléans - Bourges (All 2 available)
            # Southwest - Spain West & Atlantic (7:15 - 9:00)
            ("A-20", 6),  # Paris - Toulouse Axis
            ("A-62", 1),  # Toulouse - Bordeaux Connector (All 1 available)
            ("A-63", 3),  # Bordeaux - Biriatou (Spain Border) (All 3 available)
            ("N-10", 2),  # Bordeaux Free Route (All 2 available)
            ("A-630", 1),  # Bordeaux Ring Road (All 1 available)
            ("A-10", 7),  # Bordeaux - Paris
            # West - Brittany & Pays de la Loire (9:00 - 10:00)
            ("A-11", 3),  # Nantes - Paris (All 3 available)
            ("A-8", 2),  # Nice/Italy Coast (All 2 available - *Moved to end for flow*)
            ("A-28", 1),  # Rouen - Tours (All 1 available)
        ]
        UNKNOWN_MAPPING = {
            "12690": "A6",
            "16922": "A40",
            "2740": "A6",
            "2752": "A36",
            "2786": "A36",
            "2824": "A75",
            "5467": "A43",
            "5475": "A43",
            "5552": "A40",
            "5554": "A40",
            "5596": "BP",
            "5598": "BP",
            "5600": "BP",
            "5602": "BP",
            "5604": "BP",
            "5606": "BP",
            "at_area05": "A48",
            "at_area13": "A41",
            "camera3": "N20",
            "camera4": "N22",
            "camera5": "N22",
            "camera6": "N20",
            "camera7": "N20",
            "camera8": "N20",
            "dirco_camera24": "A20",
            "dirco_camera25": "A20",
            "dirco_camera26": "A20",
            "dirco_camera27": "A20",
            "dirco_camera28": "A20",
            "dirco_camera29": "A20",
            "dirco_camera30": "A20",
            "dirco_camera31": "N145",
            "dirco_camera32": "A10",
            "dirco_camera33": "A20",
            "dirmc_issoire_camera85": "A709",
            "dirn_lille_camera1": "A1",
            "dirn_lille_camera10": "A27",
            "dirn_lille_camera11": "A25",
            "dirn_lille_camera12": "A16",
            "dirn_lille_camera14": "A27",
            "dirn_lille_camera15": "A22",
            "dirn_lille_camera18": "A16",
            "dirn_lille_camera19": "A16",
            "dirn_lille_camera2": "A2",
            "dirn_lille_camera20": "A16",
            "dirn_lille_camera3": "A16",
            "dirn_lille_camera4": "A16",
            "dirn_lille_camera5": "A16",
            "dirn_lille_camera6": "A16",
            "dirn_lille_camera7": "A16",
            "dirn_lille_camera8": "A16",
            "dirn_lille_camera9": "A16",
            "dirno_caen_camera31": "A84",
            "dirno_caen_camera32": "A84",
            "dirno_caen_camera33": "N814",
            "dirno_caen_camera34": "N814",
            "dirno_rouen_camera61": "N338",
            "dirno_rouen_camera62": "N28",
            "dirno_rouen_camera63": "A150",
            "dirno_rouen_camera64": "A28",
            "dirno_rouen_camera65": "N154",
            "dirno_rouen_camera66": "N154",
            "dirno_rouen_camera67": "N154",
            "feche": "A36",
            "langres": "A31",
        }

        class ASFA:
            BASE_URL = "https://www.autoroutes.fr/webtrafic/desktop/webcams_en.html"
            AUTH_URL = "https://wt3.autoroutes-trafic.fr/authentication/?key={key}&base=www.autoroutes.fr&div=blocwebtrafic"
            CAMERA_SUFFIX = "webcams.js"
            VIDEO_EXT = ".flv"
            CAMERA_URL = (
                "https://gieat.viewsurf.com?id={camera_id}&action=mediaRedirect"
            )

        class HighwaySort:
            NORTH_SOUTH = [
                "A-1",
                "A-31",
                "A-6",
                "A-7",
                "A-75",
                "A-71",
                "A-20",
                "A-63",
                "N-10",
                "A-10",
                "A-28",
            ]
            EAST_WEST = [
                "A-16",
                "A-2",
                "A-26",
                "A-29",
                "A-4",
                "A-36",
                "A-40",
                "A-43",
                "A-48",
                "A-62",
                "A-11",
                "A-8",
            ]
            RINGS = ["A-630"]  # Bordeaux Ring

    class SPAIN:
        BASE_URL = "https://etraffic.dgt.es/"
        CAMERA_URL = "https://infocar.dgt.es/etraffic/data/camaras/"
        CAMERA_API = BASE_URL + "etrafficWEB/api/cache/getCamaras"
        HIGHWAY_SEQUENCE = [
            # Northern Gateways (0:00 - 1:30)
            ("A-1", 8),
            ("A-8", 5),
            # Ebro Valley & Catalonia (1:30 - 3:00)
            ("AP-68", 3),
            ("A-68", 3),
            ("Z-40", 4),
            ("AP-7", 6),
            # Mediterranean Corridor (3:00 - 4:30)
            ("A-7", 8),
            ("V-30", 5),
            # Center - Madrid Hub (4:30 - 6:30)
            ("A-3", 5),
            ("M-50", 6),
            ("M-40", 4),
            ("A-2", 4),
            # The South (6:30 - 8:00)
            ("A-4", 7),
            ("SE-30", 4),
            # The West - Portugal & Atlantic (8:00 - 10:00)
            ("A-5", 5),
            ("A-62", 6),
            ("A-6", 5),
        ]
        XOR_KEY = "K"
        IMAGE_EXT = ".jpg"
        RATE_LIMIT = 150

        class HighwaySort:
            NORTH_SOUTH = ["A-1", "AP-7", "A-7", "AP-68", "A-68", "A-6", "A-4"]
            EAST_WEST = [
                "A-8",
                "A-2",
                "A-3",
                "A-5",
                "A-62",
                "AP-68",
            ]  # AP-68 is diagonal, but X works well
            RINGS = ["Z-40", "V-30", "M-50", "M-40", "SE-30"]

    class ITALY:
        BASE_URL = "https://viabilita.autostrade.it/json/webcams.json"
        CAMERA_URL = "https://video.autostrade.it/video-mp4_hq/"
        VIDEO_EXT = ".mp4"
        RATE_LIMIT = 25
        HIGHWAY_SEQUENCE = [
            # --- BORDERS ---
            # France (Coast)
            ("A10", 4),  # Ventimiglia (Km 158) -> Genoa
            # France (Mountain)
            ("A05", 4),  # Mont Blanc (Km 143) -> Aosta -> Turin
            # France -> Milan Connector
            ("A04_WEST", 4),  # Turin - Milan
            # Switzerland
            ("A09", 5),  # Chiasso (Km 41) -> Como -> Milan
            ("A08", 3),  # Varese -> Milan (Lakes)
            # Germany/Austria
            ("A22", 8),  # Brennero (Km 0) -> Bolzano -> Verona (Km 313)
            # Austria (East)
            ("A23", 4),  # Tarvisio (Km 119) -> Udine
            # --- EAST-WEST ---
            # The "German Link" (Milan - Verona - Venice)
            ("A04_CENTER", 5),  # Brescia - Padova
            # The Eastern Gate (Venice - Trieste)
            ("A04_EAST", 5),  # Venice - Trieste
            # --- NORTH-SOUTH ---
            # Milan to Bologna
            ("A01", 6),  # Select from range Km 0 - 180
            # Bologna Hub & Adriatic (Fruit Route)
            ("A14", 4),  # Select from range Km 0 - 30 (Bologna dense area)
            ("A14", 8),  # Select from range Km 30 - 700 (Rimini - Bari)
            # The Apennines (Florence/Rome)
            ("A01", 5),  # Select from range Km 240 - 300 (Mountain Pass)
            ("A01", 5),  # Select from range Km 300 - 550 (Florence - Rome)
            # --- PORTS & SOUTH ---
            # Liguria/Tyrrhenian
            ("A12", 4),  # Genoa - Livorno
            ("A07", 3),  # Milan - Genoa
            ("A26", 3),  # Gravellona - Genoa
            # Naples (NEW DATA)
            ("A01", 3),  # Rome - Naples (Km 550 - 750)
            ("A56", 5),  # Naples Tangenziale (High traffic)
            ("A30", 3),  # Caserta - Salerno
        ]

        class A4:
            class SATAP:
                BASE_URL = "https://www.satapweb.it/en/webcam-a4/"
                CAMERA_KEYWORDS = ["<!-- WEBCAM -->", "<!-- /WEBCAM -->"]

            class ABP:
                BASE_ABP_URL = "https://inviaggio.autobspd.it"
                CAMERA_API = BASE_ABP_URL + "/o/map-rest/webcam/A4AAA"

            class CAV:
                BASE_URL = "https://www.infoviaggiando.it/"
                CAMERA_API = (
                    BASE_URL
                    + "WFS/?service=WFS&request=GetFeature&typename=PortaleWeb:VW_WEBCAM&outputFormat=json"
                )
                WEBCAM_URL = BASE_URL + "webcam/webcamimage?ipAddr={ip}&progr=1"

        class A22:
            BASE_URL = "https://www.autobrennero.it/it/"
            CAMERA_KEYWORDS = ["var puntiWebcam= ", ";var puntiBarriere"]

        class HighwaySort:
            NORTH_SOUTH = [
                "A10",
                "A05",
                "A09",
                "A08",
                "A22",
                "A23",
                "A01",
                "A14",
                "A12",
                "A07",
                "A26",
                "A30",
            ]
            EAST_WEST = ["A04", "A16"]  # A04 is the big East-West Artery
            RINGS = ["A56"]  # Naples Tangenziale

    class POLAND:
        pass

    class UK:
        EARTH_MAX_COORDS = "-180,-90,180,90"
        BASE_URL = "https://www.trafficengland.com/"
        CAMERA_API_URL = BASE_URL + f"api/cctv/getToBounds?bbox={EARTH_MAX_COORDS}"
        CAMERA_URL = (
            "https://public.highwaystrafficcameras.co.uk/cctvpublicaccess/images/"
        )
        IMAGE_EXT = ".jpg"
        RATE_LIMIT = 125
        HIGHWAY_SEQUENCE = [
            # Phase 1: Entry from Europe & London
            ("M20", 5),
            ("A282", 2),
            ("M25", 8),
            # Phase 2: The South Coast & Wales
            ("M27", 3),
            ("M3", 3),
            ("M4", 6),
            ("M5", 6),
            # Phase 3: The Midlands Hub & East Ports
            ("A14", 6),
            ("M1", 10),
            ("M42", 5),
            # Phase 4: The Northwest & Scotland Bound
            ("M6", 10),
            ("M56", 3),
            ("M60", 4),
            # Phase 5: The Pennines & North East
            ("M62", 6),
            ("A1(M)", 6),
            ("M18", 2),
        ]

        class HighwaySort:
            NORTH_SOUTH = [
                "A1",
                "A1(M)",
                "A3",
                "A3(M)",
                "A19",
                "A36",
                "A38",
                "A38(M)",
                "A42",
                "A46",
                "A168",
                "A282",
                "A419",
                "A453",
                "A556",
                "A585",
                "A1001",
                "M1",
                "M5",
                "M6",
                "M11",
                "M18",
                "M23",
                "M32",
                "M40",
                "M49",
                "M53",
                "M57",
                "M61",
                "M66",
                "M69",
                "M275",
                "M621",
            ]
            EAST_WEST = [
                "A2",
                "A5",
                "A13",
                "A14",
                "A27",
                "A30",
                "A50",
                "A64",
                "A303",
                "A421",
                "M2",
                "M3",
                "M4",
                "M20",
                "M26",
                "M27",
                "M45",
                "M48",
                "M50",
                "M54",
                "M55",
                "M56",
                "M62",
                "M65",
                "M180",
                "M602",
            ]
            RINGS = [
                "M25",  # London Orbital
                "M60",  # Manchester Outer Ring
                "M42",  # Birmingham Box (C-shaped, circular logic works best here)
            ]
