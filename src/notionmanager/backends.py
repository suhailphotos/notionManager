
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
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
# Notion Sync Backend
# -------------------------------------------------------------------
class NotionSyncBackend(BaseSyncBackend):
    def __init__(self, notion_api_key: str, notion_db_config: NotionDBConfig):
        """
        This backend requires:
         - a Notion API key
         - a NotionDBConfig object, which includes the database ID,
           forward_mapping, back_mapping, default_icon, etc.
        """
        if not notion_api_key:
            raise ValueError("Notion API key is required.")
        if not notion_db_config.database_id:
            raise ValueError("Notion database_id is required in NotionDBConfig.")

        self.notion_api_key = notion_api_key
        self.notion_db_config = notion_db_config

        # You might have a real NotionManager that uses the notion_api_key + database_id
        self.notion_manager = NotionManager(notion_api_key, notion_db_config.database_id)

        # Pre-load the pages from Notion
        self._notion_pages = self._load_notion_pages()

    def _load_notion_pages(self) -> Dict[str, dict]:
        """Retrieve pages from Notion, transform them into a dict by hash."""
        pages_raw = self.notion_manager.get_pages()
        pages_transformed = self.notion_manager.transform_pages(
            pages_raw, 
            self.notion_db_config.forward_mapping
        )
        notion_by_hash = {}
        for page in pages_transformed:
            h = page.get("hash")
            if h:
                notion_by_hash[h] = page
        return notion_by_hash

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._notion_pages

    def create_entry(self, file_info: dict):
        """
        For newly found local file: build the Notion payload using the 'back_mapping.'
        """
        flat_object = {
            "icon": self.notion_db_config.default_icon.get("icon"),
            "cover": {"type": "external", "external": {"url": file_info["cloudinary_url"]}},
            "name": file_info["display_name"],
            "image_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "path": file_info["raw_path"],
            "hash": file_info["hash"]
        }

        payload = self.notion_manager.build_notion_payload(
            flat_object, 
            self.notion_db_config.back_mapping
        )
        # If there's a top-level 'icon' in the Notion format, optionally set that.
        if flat_object.get("icon"):
            payload["icon"] = flat_object["icon"]

        self.notion_manager.add_page(payload)
        print(f"[NotionSyncBackend] Created Notion page for: {file_info['file_name']}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        """
        For an existing file that's been renamed or changed tags/paths.
        """
        flat_object = {
            "name": file_info["display_name"],
            "image_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "path": file_info["raw_path"]
        }
        # Build the partial update payload
        update_payload = self.notion_manager.build_notion_payload(
            flat_object, 
            self.notion_db_config.back_mapping
        )["properties"]

        # The "id" field in the existing entry is presumably how we identify the page:
        page_id = existing_entry.get("id")
        self.notion_manager.update_page(page_id, update_payload)
        print(f"[NotionSyncBackend] Updated Notion page for: {file_info['file_name']}")

    def delete_entry(self, existing_entry: dict):
        """For a file removed locally: remove from Notion (archive or delete)."""
        page_id = existing_entry.get("id")
        self.notion_manager.delete_page(page_id)
        print(f"[NotionSyncBackend] Deleted Notion page with hash: {existing_entry['hash']}")

# -------------------------------------------------------------------
# Local JSON Sync Backend
# -------------------------------------------------------------------
class LocalJsonSyncBackend(BaseSyncBackend):
    """
    A simple backend that stores file info in a JSON file keyed by 'hash.'
    Note that we don't need forward/back mappings, so no DB config required here.
    """
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

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._data

    def create_entry(self, file_info: dict):
        self._data[file_info["hash"]] = {
            "id": file_info["hash"],  # Some backends need an ID; here we just use hash
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "cloudinary_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "hash": file_info["hash"]
        }
        self._save_data()
        print(f"[LocalJsonSyncBackend] Created JSON entry for: {file_info['file_name']}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        entry = self._data[file_info["hash"]]
        entry.update({
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "cloudinary_url": file_info["cloudinary_url"],
            "tags": file_info["tags"]
        })
        self._save_data()
        print(f"[LocalJsonSyncBackend] Updated JSON entry for: {file_info['file_name']}")

    def delete_entry(self, existing_entry: dict):
        file_hash = existing_entry["hash"]
        if file_hash in self._data:
            del self._data[file_hash]
            self._save_data()
            print(f"[LocalJsonSyncBackend] Deleted JSON entry for hash: {file_hash}")

