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
# Stubs for your actual utilities (replace with real imports)
# -------------------------------------------------------------------
def expand_or_preserve_env_vars(path, default_value=None, keep_env_in_path=False):
    """Example stub. Replace with your actual function."""
    expanded = Path(os.path.expandvars(path))
    return expanded, str(expanded)

def compute_file_hash(file_path: Path) -> str:
    """Example stub. Replace with your actual file hashing logic."""
    import hashlib
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def generate_tags(relative_path: Path, root_category: str) -> List[str]:
    """Example stub. Replace with your actual tag-generation logic."""
    return [root_category] + list(relative_path.parts)

def create_new_url(cloudinary_url: str) -> str:
    """Example stub. Replace with your actual Cloudinary URL transform logic."""
    return cloudinary_url

# -------------------------------------------------------------------
# Stubbed NotionManager (replace with your real Notion integration)
# -------------------------------------------------------------------
class NotionManager:
    def __init__(self, api_key: str, database_id: str):
        self.api_key = api_key
        self.database_id = database_id

    def get_pages(self):
        """Fetch pages from Notion."""
        return []

    def transform_pages(self, pages, forward_mapping):
        """
        Convert pages from Notion’s shape to a uniform dict shape 
        keyed by 'hash' or something similar.
        """
        return pages  # Stub; your real code might do property extraction.

    def build_notion_payload(self, flat_object, back_mapping):
        """Convert your 'flat_object' into the Notion property structure."""
        # Stub: Return a payload that lumps everything into 'properties' for simplicity.
        return {"properties": flat_object}

    def add_page(self, payload: dict):
        print("[NotionManager] add_page called with:", payload)

    def update_page(self, page_id: str, update_payload: dict):
        print(f"[NotionManager] update_page called for page {page_id} with:", update_payload)

    def delete_page(self, page_id: str):
        print(f"[NotionManager] delete_page called for page {page_id}")

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
# The Abstract Backend
# -------------------------------------------------------------------
class BaseSyncBackend:
    """
    Abstract base class (interface) for implementing a sync backend.
    You could define more methods for full CRUD if necessary.
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


# -------------------------------------------------------------------
# Cloudinary Manager
# -------------------------------------------------------------------
class CloudinaryManager:
    """
    Scans a local folder for images, uploads them to Cloudinary if needed, and 
    uses a chosen sync backend to keep track of which files have been processed.
    """
    def __init__(
        self,
        cloud_name: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        **config
    ):
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
            print(f"Failed to set display_name metadata: {e}")

    def update_assets(
        self,
        folder_path: str,
        root_category: str,
        skip_files: Optional[List[str]] = None,
        sync_backend: Optional[BaseSyncBackend] = None,
        update_tags: bool = True
    ):
        """
        1) Scan folder 
        2) Compare with existing entries 
        3) Upload/rename in Cloudinary as needed 
        4) Create/update/delete in the chosen backend
        """
        if sync_backend is None:
            raise ValueError("No synchronization backend provided.")

        # 1) Scan
        scanned_files = self.scan_folder(folder_path, root_category, skip_files)
        # 2) Get existing entries from backend
        existing_entries = sync_backend.fetch_existing_entries()
        scanned_by_hash = {f["hash"]: f for f in scanned_files}

        # ---- Add or Update ----
        for file_hash, file_info in scanned_by_hash.items():
            cover_name = Path(file_info["file_name"]).stem.title()
            file_info["display_name"] = cover_name

            if file_hash not in existing_entries:
                # New file
                upload_resp = self.upload_file(file_info, root_category)
                file_info["cloudinary_url"] = upload_resp["secure_url"]
                self._update_display_name(upload_resp["public_id"], cover_name)
                sync_backend.create_entry(file_info)
                print(f"[CloudinaryManager] Added new entry for {file_info['file_name']}")
            else:
                # Possibly rename or update tags
                existing_entry = existing_entries[file_hash]
                old_file_name = existing_entry.get("file_name", "")
                if old_file_name != file_info["file_name"]:
                    # File rename
                    existing_cloud_url = existing_entry.get("cloudinary_url", "")
                    old_public_id = self._extract_public_id(existing_cloud_url)
                    new_public_id = f"{root_category}/{cover_name}"

                    # Attempt rename in Cloudinary
                    try:
                        rename_resp = cloudinary.uploader.rename(old_public_id, new_public_id)
                        print(f"Renamed Cloudinary asset from {old_public_id} to {new_public_id}")
                        # Refresh the URL after rename
                        resource_info = cloudinary.api.resource(new_public_id)
                        file_info["cloudinary_url"] = resource_info["secure_url"]
                    except Exception as e:
                        print(f"Failed to rename Cloudinary asset: {e}")
                        # fallback: re-upload the file
                        rename_resp = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = rename_resp["secure_url"]
                        new_public_id = rename_resp["public_id"]
                    # Update display name
                    self._update_display_name(new_public_id, cover_name)
                    # Update the backend entry
                    sync_backend.update_entry(file_info, existing_entry)
                    print(f"[CloudinaryManager] Updated entry for rename: {file_info['file_name']}")
                else:
                    # Maybe the path or tags changed
                    existing_tags = existing_entry.get("tags", [])
                    if (file_info["raw_path"] != existing_entry.get("raw_path")) \
                       or (update_tags and existing_tags != file_info["tags"]):
                        upload_resp = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = upload_resp["secure_url"]
                        self._update_display_name(upload_resp["public_id"], cover_name)
                        sync_backend.update_entry(file_info, existing_entry)
                        print(f"[CloudinaryManager] Updated entry metadata for {file_info['file_name']}")
                    else:
                        print(f"[CloudinaryManager] No changes detected for {file_info['file_name']}")

        # ---- Delete ----
        for file_hash, existing_entry in existing_entries.items():
            if file_hash not in scanned_by_hash:
                # File removed locally => remove from backend & Cloudinary
                cloud_url = existing_entry.get("cloudinary_url", "")
                public_id = self._extract_public_id(cloud_url)
                sync_backend.delete_entry(existing_entry)
                # Remove from Cloudinary
                try:
                    destroy_resp = cloudinary.uploader.destroy(public_id)
                    if destroy_resp.get("result") == "ok":
                        print(f"[CloudinaryManager] Deleted Cloudinary asset for {public_id}")
                    else:
                        print(f"[CloudinaryManager] Error deleting asset for {public_id}: {destroy_resp}")
                except Exception as e:
                    print(f"[CloudinaryManager] Failed to delete asset for {public_id}: {e}")

        print("[CloudinaryManager] Synchronization complete.")

# -------------------------------------------------------------------
# Example Usage
# -------------------------------------------------------------------
if __name__ == "__main__":

    # 1) Create a CloudinaryManager
    manager = CloudinaryManager()

    # 2) Example: Using a Local JSON backend (no forward/back mapping needed)
    local_json_backend = LocalJsonSyncBackend(json_file_path="sync_log.json")

    manager.update_assets(
        folder_path="$DROPBOX/pictures/assets/banner",
        root_category="banner",
        skip_files=None,
        sync_backend=local_json_backend,  # Use JSON backend
        update_tags=True
    )

    # 3) Example: Using a Notion backend with custom forward/back mappings
    #    Suppose we have some Notion property definitions for this "database."
    notion_forward_mapping = {
        # Example keys that might come from your pages
        "id": {"target": "id", "return": "str"},
        "Cover Name": {"target": "name", "type": "title", "return": "str"},
        "Image URL": {"target": "image_url", "type": "rich_text", "return": "str"},
        "Tags": {"target": "tags", "type": "multi_select", "return": "list"},
        "Source File Path": {"target": "path", "type": "rich_text", "return": "str"},
        "File Hash": {"target": "hash", "type": "rich_text", "return": "str"},
    }
    notion_back_mapping = {
        "name": {"target": "Cover Name", "type": "title", "return": "str"},
        "image_url": {"target": "Image URL", "type": "rich_text", "return": "str"},
        "tags": {"target": "Tags", "type": "multi_select", "return": "list"},
        "path": {"target": "Source File Path", "type": "rich_text", "return": "str"},
        "hash": {"target": "File Hash", "type": "rich_text", "return": "str"},
    }
    default_icon = {
        "icon": {
            "type": "custom_emoji",
            "custom_emoji": {
                "name": "cover-image",
                "url": "https://some-url/cover-image.svg"
            }
        }
    }

    # Create a NotionDBConfig
    notion_db_config = NotionDBConfig(
        database_id="YOUR_NOTION_DATABASE_ID",
        forward_mapping=notion_forward_mapping,
        back_mapping=notion_back_mapping,
        default_icon=default_icon
    )

    # Create a NotionSyncBackend with an API key + the config above.
    # notion_backend = NotionSyncBackend(
    #     notion_api_key="YOUR_NOTION_API_KEY",
    #     notion_db_config=notion_db_config
    # )

    # manager.update_assets(
    #     folder_path="$DROPBOX/pictures/assets/banner",
    #     root_category="banner",
    #     skip_files=None,
    #     sync_backend=notion_backend,  # Use Notion backend
    #     update_tags=True
    # )
