import json
import os

MAPPING_FILE = "group_mapping.json"

def save_mapping(mapping):
    try:
        with open(MAPPING_FILE, 'w') as f:
            json.dump(mapping, f, indent=4)
        return True
    except Exception as e:
        print(f"Error saving mapping: {e}")
        return False

def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        return {}
    try:
        with open(MAPPING_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading mapping: {e}")
        return {}
