import os
import re
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api

from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any

# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------

from notionmanager.utils import hide_file

# -------------------------------------------------------------------
# NotionManager
# -------------------------------------------------------------------

from notionmanager.notion import NotionManager


# -------------------------------------------------------------------
# The Abstract Backend
# -------------------------------------------------------------------
class BaseSyncBackend:
    """
    Abstract base class (interface) for implementing a sync backend.
    Define more methods for full CRUD if necessary.
    """
    def fetch_existing_entries(self) -> Dict[str, dict]:
        """
        Return a dict keyed by file-hash with existing entries.
        """
        raise NotImplementedError

    def create_entry(self, file_info: dict):
        """
        Called when a new file is found locally (not in the backend).
        """
        raise NotImplementedError

    def update_entry(self, file_info: dict, existing_entry: dict):
        """
        Called when we have a local file that already exists in the backend
        but some attributes changed.
        """
        raise NotImplementedError

    def delete_entry(self, existing_entry: dict):
        """
        Called when an entry exists in the backend but is removed locally.
        """
        raise NotImplementedError

# -------------------------------------------------------------------
# Config object for a Notion database
# -------------------------------------------------------------------
class NotionDBConfig:
    """
    Holds 'one unit' of Notion-specific config:
    - notion_database_id
    - forward_mapping
    - back_mapping
    - any other details (e.g., default icon, etc.)
    """
    def __init__(
        self,
        database_id: str,
        forward_mapping: Dict[str, Dict[str, Any]],
        back_mapping: Dict[str, Dict[str, Any]],
        default_icon: Optional[dict] = None
    ):
        self.database_id = database_id
        self.forward_mapping = forward_mapping
        self.back_mapping = back_mapping
        self.default_icon = default_icon or {}


# -------------------------------------------------------------------
# NotionSyncBackend
# -------------------------------------------------------------------
class NotionSyncBackend(BaseSyncBackend):
    def __init__(self, notion_api_key: str, notion_db_config: NotionDBConfig):
        if not notion_api_key:
            raise ValueError("Notion API key required.")
        if not notion_db_config.database_id:
            raise ValueError("Notion DB ID required.")
        
        self.notion_api_key = notion_api_key
        self.notion_db_config = notion_db_config
        self.notion_manager = NotionManager(notion_api_key, notion_db_config.database_id)
        self._notion_pages = self._load_notion_pages()

    def _load_notion_pages(self) -> Dict[str, dict]:
        pages_raw = self.notion_manager.get_pages()
        transformed = self.notion_manager.transform_pages(
            pages_raw,
            self.notion_db_config.forward_mapping
        )
        # find sync_key
        sync_local_key = None
        for local_key, cfg in self.notion_db_config.forward_mapping.items():
            if cfg.get("sync_key") is True:
                sync_local_key = cfg.get("target")
                break
        if not sync_local_key:
            raise ValueError("No sync_key found in forward_mapping.")
        
        notion_by_key = {}
        for page in transformed:
            unique_val = page.get(sync_local_key)
            if unique_val:
                notion_by_key[unique_val] = page
        return notion_by_key

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._notion_pages

    def _build_flat_object_for_create(self, file_info: dict) -> dict:
        flat_object = {}
        # If the mapping expects an icon, then use file_info["icon"] if present;
        # otherwise, if a default icon is defined in the config, assign the whole dictionary.
        if "icon" in self.notion_db_config.back_mapping:
            if "icon" in file_info:
                flat_object["icon"] = file_info["icon"]
            elif self.notion_db_config.default_icon:
                flat_object["icon"] = self.notion_db_config.default_icon  # assign the entire default icon dictionary
    
        # Loop through each field defined in the back mapping.
        for local_key in self.notion_db_config.back_mapping.keys():
            if local_key == "icon":
                continue  # already handled above
            elif local_key == "cover":
                # Use the transformed image URL for the cover.
                if "image_url" in file_info:
                    flat_object["cover"] = {
                        "type": "external",
                        "external": {"url": file_info["image_url"]}
                    }
            else:
                # For keys like 'name', 'image_url', 'tags', 'path', and 'hash'
                # Note: Ensure that the file_info key for source file path is "path" (manager should copy raw_path to path).
                if local_key in file_info:
                    flat_object[local_key] = file_info[local_key]
        return flat_object
    
    def _build_flat_object_for_update(self, file_info: dict, existing_entry: dict) -> dict:
        flat_object = {}
        # For icon: if provided in file_info, use it; otherwise use the default if available.
        if "icon" in self.notion_db_config.back_mapping:
            if "icon" in file_info:
                flat_object["icon"] = file_info["icon"]
            elif self.notion_db_config.default_icon:
                flat_object["icon"] = self.notion_db_config.default_icon
        # For cover: use the current transformed image_url.
        if "cover" in self.notion_db_config.back_mapping:
            if "image_url" in file_info:
                flat_object["cover"] = {
                    "type": "external",
                    "external": {"url": file_info["image_url"]}
                }
        for local_key in self.notion_db_config.back_mapping.keys():
            if local_key in ("icon", "cover"):
                continue
            if local_key in file_info:
                flat_object[local_key] = file_info[local_key]
        return flat_object


    def create_entry(self, file_info: dict):
        flat_object = self._build_flat_object_for_create(file_info)
        payload = self.notion_manager.build_notion_payload(
            flat_object,
            self.notion_db_config.back_mapping
        )
        if "icon" in flat_object:
            payload["icon"] = flat_object["icon"]
        if "cover" in flat_object:
            payload["cover"] = flat_object["cover"]
        self.notion_manager.add_page(payload)
        print(f"[NotionSyncBackend] Created Notion page for {file_info.get('file_name')}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        # Build the flat object using our back mapping.
        flat_object = self._build_flat_object_for_update(file_info, existing_entry)
        # Build the full payload using our reverse mapping.
        notion_payload = self.notion_manager.build_notion_payload(
            flat_object,
            self.notion_db_config.back_mapping
        )
        page_id = existing_entry.get("id")
        
        # Update properties:
        # Extract only the "properties" part from the payload.
        properties = notion_payload.get("properties", {})
        self.notion_manager.update_page(page_id, properties)
        print(f"[NotionSyncBackend] Updated properties for {file_info.get('file_name')}")
        
        # Update cover if provided.
        if "cover" in flat_object:
            cover_payload = flat_object["cover"]
            self.notion_manager.update_cover(page_id, cover_payload)
            print(f"[NotionSyncBackend] Updated cover for {file_info.get('file_name')}")
        
        # Update icon if provided.
        if "icon" in flat_object:
            icon_payload = flat_object["icon"]
            self.notion_manager.update_icon(page_id, icon_payload)
            print(f"[NotionSyncBackend] Updated icon for {file_info.get('file_name')}")


    def delete_entry(self, existing_entry: dict):
        page_id = existing_entry.get("id")
        self.notion_manager.delete_page(page_id)
        print(f"[NotionSyncBackend] Deleted Notion page with hash {existing_entry.get('hash')}")

# -------------------------------------------------------------------
# LocalJsonSyncBackend
# -------------------------------------------------------------------
class LocalJsonSyncBackend(BaseSyncBackend):
    def __init__(self, json_file_path: str):
        self.json_file_path = Path(json_file_path)
        self._data = self._load_data()

    def _load_data(self) -> Dict[str, dict]:
        if self.json_file_path.exists():
            with open(self.json_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_data(self):
        with open(self.json_file_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

        hide_file(self.json_file_path)

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._data

    def create_entry(self, file_info: dict):
        self._data[file_info["hash"]] = {
            "id": file_info["hash"],
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "image_url": file_info.get("image_url"),
            "tags": file_info.get("tags", []),
            "hash": file_info["hash"]
        }
        self._save_data()
        print(f"[LocalJsonSyncBackend] Created entry for {file_info['file_name']}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        old_hash = existing_entry["hash"]          # hash stored in the log
        new_hash = file_info["hash"]               # current (possibly new) hash
    
        # 1. Fetch the existing record using the OLD hash
        record = self._data.pop(old_hash, {})      # safely remove; returns {} if missing
    
        # 2. Update / replace fields
        record.update({
            "id": new_hash,                        # keep id in‑sync with hash
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "image_url": file_info.get("image_url"),
            "tags": file_info.get("tags", []),
            "hash": new_hash
        })
    
        # 3. Re‑insert under the NEW hash key
        self._data[new_hash] = record
    
        # 4. Persist to disk
        self._save_data()
        print(f"[LocalJsonSyncBackend] Updated entry for {file_info['file_name']}")

    def delete_entry(self, existing_entry: dict):
        file_hash = existing_entry["hash"]
        if file_hash in self._data:
            del self._data[file_hash]
            self._save_data()
            print(f"[LocalJsonSyncBackend] Deleted entry for hash: {file_hash}")


if __name__ == "__main__":
    import argparse
    from notionmanager.config import load_sync_config
    # -------------------------------------------------------------------
    # Load environment variables from .env file
    # -------------------------------------------------------------------
    env_path = Path(__file__).parent / ".env"
    prod_env_path = Path.home() / ".notionmanager" / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print("Loaded .env from:", env_path)
    elif prod_env_path.exists():
        load_dotenv(prod_env_path)
        print("Loaded .env from:", prod_env_path)
    else:
        print("No .env file found; using system environment variables.")

    # -------------------------------------------------------------------
    # Load the sync configuration using load_sync_config()
    # -------------------------------------------------------------------
    try:
        config_data = load_sync_config()
    except Exception as e:
        print("Error loading sync configuration:", e)
        exit(1)

    sync_jobs = config_data.get("sync_jobs", [])
    if not sync_jobs:
        print("No sync_jobs found in config.")
        exit(0)

    # -------------------------------------------------------------------
    # Setup argparse to optionally test one sync job by name
    # -------------------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Test NotionSyncBackend and LocalJsonSyncBackend from sync_config.json."
    )
    parser.add_argument(
        "--job",
        help="Name of the sync job to test (e.g., 'banner' or 'gicon'). If omitted, all jobs are tested."
    )
    args = parser.parse_args()

    if args.job:
        sync_jobs = [job for job in sync_jobs if job.get("name") == args.job]
        if not sync_jobs:
            print("No sync job found with name:", args.job)
            exit(0)

    # -------------------------------------------------------------------
    # Iterate over the sync jobs from the config and test each backend
    # -------------------------------------------------------------------
    for job in sync_jobs:
        job_name = job.get("name")
        folder_path = job.get("path")
        method = job.get("method", {})
        method_type = method.get("type")

        print("\n=== Testing sync job: '{}' ===".format(job_name))

        if method_type == "notiondb":
            # Test NotionSyncBackend
            notiondb_cfg = method.get("notiondb", {})
            notion_forward = method.get("forward_mapping", {})
            notion_reverse = method.get("reverse_mapping", {})
            db_id = notiondb_cfg.get("id")
            default_icon = notiondb_cfg.get("default_icon", {})

            # Get the Notion API key from environment variables
            notion_api_key = os.getenv("NOTION_API_KEY", "YOUR_NOTION_API_KEY")
            try:
                notion_db_config = NotionDBConfig(
                    database_id=db_id,
                    forward_mapping=notion_forward,
                    back_mapping=notion_reverse,
                    default_icon=default_icon
                )
                notion_backend = NotionSyncBackend(notion_api_key, notion_db_config)
                entries = notion_backend.fetch_existing_entries()
                print("Fetched entries from NotionSyncBackend:")
                print(entries)
                if not entries:
                    print("Warning: fetch_existing_entries returned empty. Verify your Notion database and that the forward_mapping contains a valid 'sync_key'.")
            except Exception as e:
                print("Error testing NotionSyncBackend:", e)

        elif method_type == "jsonlog":
            # Test LocalJsonSyncBackend
            jsonlog_cfg = method.get("jsonlog", {})
            log_file_name = jsonlog_cfg.get("file_name", "sync_log.json")
            in_folder = jsonlog_cfg.get("in_folder", True)
            log_path = jsonlog_cfg.get("log_path", "")

            # Determine the JSON log file path
            if in_folder:
                # Expand environment variables (e.g., $DROPBOX) in folder_path
                expanded_folder = os.path.expandvars(folder_path)
                log_json_path = Path(expanded_folder) / log_file_name
            else:
                log_json_path = Path(log_path) / log_file_name if log_path else Path(log_file_name)

            try:
                from notionmanager.backends import LocalJsonSyncBackend  # adjust import as needed

                json_backend = LocalJsonSyncBackend(str(log_json_path))

                print("Initial entries in LocalJsonSyncBackend:")
                entries = json_backend.fetch_existing_entries()
                print(entries)

                # Create a test entry
                test_file_info = {
                    "hash": "testhash123",
                    "file_name": "test_file.jpg",
                    "raw_path": str(Path(os.path.expandvars(folder_path)) / "test_file.jpg"),
                    "image_url": "http://example.com/test_file.jpg",
                    "tags": ["test", "sync"]
                }
                print("\nCreating test entry in JSON log backend...")
                json_backend.create_entry(test_file_info)
                entries = json_backend.fetch_existing_entries()
                print("Entries after creation:")
                print(entries)

                # Update the test entry
                test_file_info_updated = test_file_info.copy()
                test_file_info_updated["file_name"] = "updated_test_file.jpg"
                print("\nUpdating test entry in JSON log backend...")
                existing_entry = entries.get("testhash123", {})
                json_backend.update_entry(test_file_info_updated, existing_entry)
                entries = json_backend.fetch_existing_entries()
                print("Entries after update:")
                print(entries)

                # Delete the test entry
                print("\nDeleting test entry in JSON log backend...")
                existing_entry = entries.get("testhash123", {})
                json_backend.delete_entry(existing_entry)
                entries = json_backend.fetch_existing_entries()
                print("Entries after deletion:")
                print(entries)

            except Exception as e:
                print("Error testing LocalJsonSyncBackend:", e)

        else:
            print("Unknown method type:", method_type)

    print("\n[Main] Sync backend tests completed.")
