import json
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any


# -------------------------------------------------------------------
# Read Sync Config
# -------------------------------------------------------------------
def load_sync_config() -> dict:
    """
    Loads sync_config.json from either a dev or prod location.
    Returns a dict of the entire config, containing "sync_jobs".
    """
    dev_config_path = Path(__file__).parent / ".config" / "sync_config.json"
    prod_config_path = Path.home() / ".notionmanager" / "sync_config.json"

    config_path = None
    if prod_config_path.exists():
        config_path = prod_config_path
    elif dev_config_path.exists():
        config_path = dev_config_path
    
    if not config_path:
        raise FileNotFoundError("sync_config.json not found in dev or prod paths.")
    
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def load_notiondb_config(db_name_or_id: str) -> Dict[str, Any]:
    """
    Loads the notiondb_config.json from either a dev or prod location.
    Then searches for the database object matching db_name_or_id (which could be the 'id' or 'name').
    
    Returns the entire dict for that database.
    
    Raises ValueError if no matching DB is found.
    """
    dev_config_path = Path(__file__).parent / ".config" / "notiondb_config.json"
    prod_config_path = Path.home() / ".notionmanager" / "notiondb_config.json"
    
    config_path = None
    if dev_config_path.exists():
        config_path = dev_config_path
    elif prod_config_path.exists():
        config_path = prod_config_path
    
    if not config_path:
        raise FileNotFoundError("Could not locate notiondb_config.json in either .config or ~/.notionmanager.")
    
    with open(config_path, "r", encoding="utf-8") as f:
        full_config = json.load(f)
    
    databases = full_config.get("databases", [])
    for db_obj in databases:
        if db_obj.get("id") == db_name_or_id or db_obj.get("name") == db_name_or_id:
            return db_obj
    
    raise ValueError(f"No database found in config matching: {db_name_or_id}")


if __name__ == "__main__":
    def test_load_notiondb_config():
        try:
            # Test case with a valid database name or ID (modify as needed)
            test_db_name = "Cover Images"  # Replace with an actual DB name or ID from your JSON
            config = load_notiondb_config(test_db_name)
            print("Database Config Found:", json.dumps(config, indent=2))
        
        except FileNotFoundError as e:
            print("Error:", e)
        
        except ValueError as e:
            print("Error:", e)

    def test_sync_config():
        try:
            config_data = load_sync_config()
            sync_jobs = config_data.get("sync_jobs", [])
        except FileNotFoundError as e:
            print("Error: ", e)

        print(json.dumps(config_data, indent=2))

    test_sync_config()
