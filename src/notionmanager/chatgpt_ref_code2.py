import os
import re
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api

from pathlib import Path
from dotenv import load_dotenv  # Only used here for .env, not for JSON
from typing import List, Optional, Dict, Any


# -------------------------------------------------------------------
# 1) Utility Functions
# -------------------------------------------------------------------
def expand_or_preserve_env_vars(path, default_value=None, keep_env_in_path=False):
    """Expand environment variables in a path (stub)."""
    expanded = Path(os.path.expandvars(path))
    return expanded, str(expanded)

def compute_file_hash(file_path: Path) -> str:
    """Compute an MD5 hash of the file."""
    import hashlib
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def generate_tags(relative_path: Path, root_category: str) -> List[str]:
    """Generate tags from the file path + root category."""
    return [root_category] + list(relative_path.parts)

def create_new_url(cloudinary_url: str) -> str:
    """Transform the Cloudinary URL if needed (stub)."""
    return cloudinary_url


# -------------------------------------------------------------------
# 2) Reading NotionDB Config from JSON
# -------------------------------------------------------------------
def load_notiondb_config(db_name_or_id: str) -> Dict[str, Any]:
    """
    Loads the notiondb_config.json from either a dev or prod location.
    Then searches for the database object matching db_name_or_id (which could be the 'id' or 'name').
    
    Returns the entire dict for that database, e.g.:
    {
      "id": "...",
      "name": "...",
      "default_icon": {...},
      "forward_mapping": {...},
      "reverse_mapping": {...}
    }
    
    Raises ValueError if no matching DB is found.
    """
    dev_config_path = Path(__file__).parent / ".config" / "notiondb_config.json"
    prod_config_path = Path.home() / ".notionmanager" / "notiondb_config.json"
    
    # In real code, you might set up your own logic to load .env if you want,
    # but here we’re focusing on reading JSON, so let's do standard file I/O.
    
    config_path = None
    if dev_config_path.exists():
        config_path = dev_config_path
    elif prod_config_path.exists():
        config_path = prod_config_path
    
    if not config_path:
        raise FileNotFoundError("Could not locate notiondb_config.json in either .config or ~/.notionmanager.")
    
    with open(config_path, "r", encoding="utf-8") as f:
        full_config = json.load(f)
    
    # full_config is expected to have "databases" as a list
    databases = full_config.get("databases", [])
    for db_obj in databases:
        if db_obj.get("id") == db_name_or_id or db_obj.get("name") == db_name_or_id:
            return db_obj
    
    raise ValueError(f"No database found in config matching: {db_name_or_id}")


# -------------------------------------------------------------------
# 3) Stubbed NotionManager (unchanged from your existing code)
# -------------------------------------------------------------------
class NotionManager:
    def __init__(self, api_key: str, database_id: str):
        self.api_key = api_key
        self.database_id = database_id

    def get_pages(self):
        # Stub: Return an empty list
        return []

    def transform_pages(self, pages, mapping):
        # Stub: Return pages as is
        return pages

    def build_notion_payload(self, flat_object, back_mapping):
        # Stub: wrap flat_object in "properties"
        return {"properties": flat_object}

    def add_page(self, payload):
        print("[NotionManager] Adding page with payload:", payload)

    def update_page(self, page_id, update_payload):
        print(f"[NotionManager] Updating page {page_id} with payload:", update_payload)

    def delete_page(self, page_id):
        print(f"[NotionManager] Deleting page {page_id}")


# -------------------------------------------------------------------
# 4) The Abstract Backend
# -------------------------------------------------------------------
class BaseSyncBackend:
    def fetch_existing_entries(self) -> Dict[str, dict]:
        raise NotImplementedError

    def create_entry(self, file_info: dict):
        raise NotImplementedError

    def update_entry(self, file_info: dict, existing_entry: dict):
        raise NotImplementedError

    def delete_entry(self, existing_entry: dict):
        raise NotImplementedError


# -------------------------------------------------------------------
# 5) NotionDBConfig Class
# -------------------------------------------------------------------
class NotionDBConfig:
    """
    Holds 'one unit' of Notion-specific config:
    - database_id
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
# 6) NotionSyncBackend Using NotionDBConfig
# -------------------------------------------------------------------
class NotionSyncBackend(BaseSyncBackend):
    def __init__(self, notion_api_key: str, db_config: NotionDBConfig):
        if not notion_api_key:
            raise ValueError("Notion API key is required.")
        if not db_config.database_id:
            raise ValueError("Notion database_id is required.")

        self.notion_api_key = notion_api_key
        self.db_config = db_config
        self.notion_manager = NotionManager(notion_api_key, db_config.database_id)
        self._notion_pages = self._load_notion_pages()

    def _load_notion_pages(self) -> Dict[str, dict]:
        pages_raw = self.notion_manager.get_pages()
        # Transform with the forward_mapping
        transformed = self.notion_manager.transform_pages(
            pages_raw,
            self.db_config.forward_mapping
        )
        # Build dict keyed by "hash" (or "sync_key" if that's your naming):
        notion_by_hash = {}
        for page in transformed:
            h = page.get("hash")
            if h:
                notion_by_hash[h] = page
        return notion_by_hash

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._notion_pages

    def create_entry(self, file_info: dict):
        flat_object = {
            "icon": self.db_config.default_icon.get("icon"),
            "cover": {"type": "external", "external": {"url": file_info["cloudinary_url"]}},
            "name": file_info["display_name"],
            "image_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "path": file_info["raw_path"],
            "hash": file_info["hash"]  # or "sync_key"
        }
        payload = self.notion_manager.build_notion_payload(
            flat_object,
            self.db_config.back_mapping
        )
        # If we have an icon defined at top-level:
        if flat_object.get("icon"):
            payload["icon"] = flat_object["icon"]
        self.notion_manager.add_page(payload)
        print(f"[NotionSyncBackend] Created Notion page for {file_info['file_name']}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        flat_object = {
            "name": file_info["display_name"],
            "image_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "path": file_info["raw_path"]
        }
        update_payload = self.notion_manager.build_notion_payload(
            flat_object, self.db_config.back_mapping
        )["properties"]
        page_id = existing_entry.get("id")
        self.notion_manager.update_page(page_id, update_payload)
        print(f"[NotionSyncBackend] Updated Notion page for {file_info['file_name']}")

    def delete_entry(self, existing_entry: dict):
        page_id = existing_entry.get("id")
        self.notion_manager.delete_page(page_id)
        print(f"[NotionSyncBackend] Deleted Notion page with hash: {existing_entry['hash']}")


# -------------------------------------------------------------------
# 7) Local JSON Sync Backend
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

    def fetch_existing_entries(self) -> Dict[str, dict]:
        return self._data

    def create_entry(self, file_info: dict):
        # Key by file_info["hash"]
        self._data[file_info["hash"]] = {
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "cloudinary_url": file_info["cloudinary_url"],
            "tags": file_info["tags"],
            "hash": file_info["hash"]
        }
        self._save_data()
        print(f"[LocalJsonSyncBackend] Created entry for {file_info['file_name']}")

    def update_entry(self, file_info: dict, existing_entry: dict):
        self._data[file_info["hash"]].update({
            "file_name": file_info["file_name"],
            "raw_path": file_info["raw_path"],
            "cloudinary_url": file_info["cloudinary_url"],
            "tags": file_info["tags"]
        })
        self._save_data()
        print(f"[LocalJsonSyncBackend] Updated entry for {file_info['file_name']}")

    def delete_entry(self, existing_entry: dict):
        file_hash = existing_entry["hash"]
        if file_hash in self._data:
            del self._data[file_hash]
            self._save_data()
            print(f"[LocalJsonSyncBackend] Deleted entry for hash: {file_hash}")


# -------------------------------------------------------------------
# 8) Cloudinary Manager
# -------------------------------------------------------------------
class CloudinaryManager:
    """
    Handles scanning a local folder and uploading to Cloudinary, then calling
    the appropriate backend to track changes.
    """
    def __init__(self,
                 cloud_name: Optional[str] = None,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 **config):
        env_path = Path(__file__).parent / ".env"
        prod_env_path = Path.home() / ".notionmanager" / ".env"

        if env_path.exists():
            load_dotenv(env_path)
        elif prod_env_path.exists():
            load_dotenv(prod_env_path)

        self.cloud_name = cloud_name or os.getenv("CLOUDINARY_CLOUD_NAME")
        self.api_key = api_key or os.getenv("CLOUDINARY_API_KEY")
        self.api_secret = api_secret or os.getenv("CLOUDINARY_API_SECRET")

        if not all([self.cloud_name, self.api_key, self.api_secret]):
            raise ValueError("Cloudinary credentials missing. Provide manually or in .env.")

        cloudinary.config(
            cloud_name=self.cloud_name,
            api_key=self.api_key,
            api_secret=self.api_secret,
            **config
        )

    def _extract_public_id(self, url: str) -> str:
        try:
            pattern = r"/upload/(?:[^/]+/)?v\d+/([^\.]+)\."
            match = re.search(pattern, url)
            return match.group(1) if match else ""
        except Exception as e:
            print(f"Error extracting public_id: {e}")
            return ""

    def scan_folder(self, folder_path: str, root_category: str,
                    skip_files: Optional[List[str]] = None) -> List[dict]:
        skip_files = skip_files or []
        expanded_folder_path, raw_folder_path = expand_or_preserve_env_vars(
            folder_path, None, keep_env_in_path=True
        )
        if not expanded_folder_path.exists():
            raise FileNotFoundError(f"Folder {expanded_folder_path} does not exist.")

        supported_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".avif"}
        if root_category == "icon":
            supported_extensions.add(".svg")

        files_data = []
        for file in expanded_folder_path.rglob("*"):
            if file.suffix.lower() in supported_extensions and file.name not in skip_files:
                file_hash = compute_file_hash(file)
                relative_path = file.relative_to(expanded_folder_path)
                tags = generate_tags(relative_path, root_category)
                raw_file_path = os.path.join(raw_folder_path, str(relative_path))
                files_data.append({
                    "file_name": file.name,
                    "raw_path": raw_file_path,
                    "expanded_path": str(file),
                    "hash": file_hash,
                    "tags": tags
                })
        return files_data

    def upload_file(self, file_info: dict, root_category: str) -> dict:
        file_path = Path(file_info["expanded_path"])
        return cloudinary.uploader.upload(
            str(file_path),
            folder=f"{root_category}/",
            tags=file_info["tags"],
            use_filename=True,
            unique_filename=False
        )

    def _update_display_name(self, public_id: str, display_name: str):
        try:
            cloudinary.uploader.update_metadata(
                {"display_name": display_name},
                public_ids=[public_id]
            )
            print(f"Set display_name='{display_name}' on public_id={public_id}")
        except Exception as e:
            print(f"Failed to set display_name: {e}")

    def update_assets(
        self,
        folder_path: str,
        root_category: str,
        skip_files: Optional[List[str]] = None,
        sync_backend: Optional[BaseSyncBackend] = None,
        update_tags: bool = True
    ):
        if sync_backend is None:
            raise ValueError("No synchronization backend provided.")

        # 1) Scan local folder
        scanned_files = self.scan_folder(folder_path, root_category, skip_files)
        # 2) Fetch existing entries from backend
        existing_entries = sync_backend.fetch_existing_entries()
        scanned_by_hash = {f["hash"]: f for f in scanned_files}

        # --- Process additions or updates ---
        for file_hash, file_info in scanned_by_hash.items():
            cover_name = Path(file_info["file_name"]).stem.title()
            file_info["display_name"] = cover_name

            if file_hash not in existing_entries:
                # New file
                response = self.upload_file(file_info, root_category)
                file_info["cloudinary_url"] = response["secure_url"]
                self._update_display_name(response["public_id"], cover_name)
                sync_backend.create_entry(file_info)
                print(f"[CloudinaryManager] Added new entry for {file_info['file_name']}")
            else:
                # Possibly rename or update tags/path
                existing_entry = existing_entries[file_hash]
                old_file_name = existing_entry.get("file_name", "")
                if old_file_name != file_info["file_name"]:
                    # rename
                    existing_cloud_url = existing_entry.get("cloudinary_url", "")
                    old_public_id = self._extract_public_id(existing_cloud_url)
                    new_public_id = f"{root_category}/{cover_name}"

                    try:
                        rename_resp = cloudinary.uploader.rename(old_public_id, new_public_id)
                        print(f"[CloudinaryManager] Renamed asset from {old_public_id} to {new_public_id}")
                        # Refresh
                        resource_info = cloudinary.api.resource(new_public_id)
                        file_info["cloudinary_url"] = resource_info["secure_url"]
                    except Exception as e:
                        print(f"[CloudinaryManager] Rename failed: {e}")
                        # fallback: reupload
                        rename_resp = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = rename_resp["secure_url"]
                        new_public_id = rename_resp["public_id"]
                    self._update_display_name(new_public_id, cover_name)
                    sync_backend.update_entry(file_info, existing_entry)
                    print(f"[CloudinaryManager] Updated entry (rename) for {file_info['file_name']}")
                else:
                    existing_tags = existing_entry.get("tags", [])
                    path_changed = (file_info["raw_path"] != existing_entry.get("raw_path"))
                    tags_changed = (update_tags and existing_tags != file_info["tags"])
                    if path_changed or tags_changed:
                        response = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = response["secure_url"]
                        self._update_display_name(response["public_id"], cover_name)
                        sync_backend.update_entry(file_info, existing_entry)
                        print(f"[CloudinaryManager] Updated entry metadata for {file_info['file_name']}")
                    else:
                        print(f"[CloudinaryManager] No changes for {file_info['file_name']}")

        # --- Process deletions ---
        for file_hash, existing_entry in existing_entries.items():
            if file_hash not in scanned_by_hash:
                # File was removed locally
                cloud_url = existing_entry.get("cloudinary_url", "")
                public_id = self._extract_public_id(cloud_url)
                sync_backend.delete_entry(existing_entry)

                try:
                    destroy_resp = cloudinary.uploader.destroy(public_id)
                    if destroy_resp.get("result") == "ok":
                        print(f"[CloudinaryManager] Deleted Cloudinary asset {public_id}")
                    else:
                        print(f"[CloudinaryManager] Error deleting asset for {public_id}: {destroy_resp}")
                except Exception as e:
                    print(f"[CloudinaryManager] Failed to delete asset for {public_id}: {e}")

        print("[CloudinaryManager] Synchronization complete.")


# -------------------------------------------------------------------
# 9) Example Usage
# -------------------------------------------------------------------
if __name__ == "__main__":

    # (A) Use Local JSON as a backend
    manager = CloudinaryManager()
    local_json_backend = LocalJsonSyncBackend(json_file_path="sync_log.json")

    manager.update_assets(
        folder_path="$DROPBOX/pictures/assets/banner",
        root_category="banner",
        sync_backend=local_json_backend,
        update_tags=True
    )

    # (B) Use Notion as a backend, loading from notiondb_config.json
    # For demonstration, let's say the user wants the DB with name "Cover Images"
    # or "154a1865-b187-8082-9bd2-c4349fb0c736"
    try:
        db_obj = load_notiondb_config("Cover Images")  # or pass the DB ID
        # db_obj might look like:
        # {
        #    "id": "154a1865-...",
        #    "default_icon": {...},
        #    "forward_mapping": {...},
        #    "reverse_mapping": {...},
        #    ...
        # }

        # Build NotionDBConfig from db_obj
        db_config = NotionDBConfig(
            database_id=db_obj["id"],
            forward_mapping=db_obj["forward_mapping"],
            back_mapping=db_obj["reverse_mapping"],
            default_icon=db_obj.get("default_icon", {})
        )
        # If you have your Notion API key in .env or environment variables
        notion_api_key = os.getenv("NOTION_API_KEY") or "YOUR_NOTION_API_KEY"

        # Create the backend
        notion_backend = NotionSyncBackend(notion_api_key, db_config=db_config)

        # Then sync
        manager.update_assets(
            folder_path="$DROPBOX/pictures/assets/banner",
            root_category="banner",
            sync_backend=notion_backend,
            update_tags=True
        )
    except Exception as e:
        print("[Main] Unable to load or sync with Notion:", e)
