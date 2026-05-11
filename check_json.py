import json
from pathlib import Path

def json_to_dict(file_path: Path):
    with Path.open(file_path, "r") as f:
        file_dict = json.load(f)
    return file_dict

def get_alerts_causes(json_dict: dict):
    management_types = set()
    cause_types = set()
    detailed_cause_types = set()
    full_types = set()
    for alert in json_dict["alerts"]:
        alert_management_type = alert.get("management_type", None)
        alert_cause_type = alert.get("cause_type", None)
        alert_detailed_cause_type = alert.get("detailed_cause_type", None)
        alert_full_type = f"{alert_management_type};{alert_cause_type};{alert_detailed_cause_type}"
        management_types.add(alert_management_type)
        cause_types.add(alert_cause_type)
        detailed_cause_types.add(alert_detailed_cause_type)
        full_types.add(alert_full_type)
    return management_types, cause_types, detailed_cause_types, full_types

def list_alert_causes(json_dict: dict):
    m, c, d, f = get_alerts_causes(json_dict)
    print("--------------")
    print("management types:")
    print("--------------")
    for management_type in m:
        print(management_type)
    print("--------------")
    print("cause types:")
    print("--------------")
    for cause_type in c:
        print(cause_type)
    print("--------------")
    print("detailed cause types:")
    print("--------------")
    for detailed_cause_type in d:
        print(detailed_cause_type)
    print("--------------")
    print("full types:")
    print("--------------")
    for full_type in f:
        print(full_type)

def get_roads(json_dict: dict):
    roads = set()
    for alert in json_dict["alerts"]:
        road_name = alert.get("road_name", '') 
        roads.add(road_name)
    return roads



if __name__ == "__main__":
    json_file = Path("data/overlay_spain/overlay_data.json")
    json_dict = json_to_dict(json_file)
   # list_alert_causes(json_dict)
   # roads = get_roads(json_dict)


    


