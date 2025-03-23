import os
import re
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api
import cloudinary.utils

from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any

# -------------------------------------------------------------------
# Import helper functions from utils module.
# -------------------------------------------------------------------

from notionmanager.utils import (
    expand_or_preserve_env_vars,
    compute_file_hash,
    generate_tags,
    create_new_url
)

# -------------------------------------------------------------------
# NotionManager
# -------------------------------------------------------------------

from notionmanager.notion import NotionManager

# -------------------------------------------------------------------
# Backends
# -------------------------------------------------------------------

from notionmanager.backends import (
    BaseSyncBackend,
    NotionDBConfig,
    NotionSyncBackend,
    LocalJsonSyncBackend
)

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

from notionmanager.config import load_sync_config

# -------------------------------------------------------------------
# CloudinaryManager
# -------------------------------------------------------------------

class CloudinaryManager:
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
            raise ValueError("Cloudinary credentials missing.")

        cloudinary.config(
            cloud_name=self.cloud_name,
            api_key=self.api_key,
            api_secret=self.api_secret,
            **config
        )

    def _extract_public_id(self, url: str) -> str:
        """
        Extracts the public_id from a Cloudinary URL, ignoring transformation parameters.
        For example:
          https://res.cloudinary.com/dicttuyma/image/upload/w_1500,h_600,c_fill,g_auto/v1742155960/banner/abstract_18.jpg
        returns: "banner/abstract_18"
        """
        try:
            pattern = r"/upload/(?:[^/]+/)?v\d+/([^\.]+)\."
            match = re.search(pattern, url)
            return match.group(1) if match else ""
        except Exception as e:
            print(f"Error extracting public_id: {e}")
            return ""

    def get_asset_url(self, public_id: str, **options) -> str:
        """
        Generate a secure URL for the given asset public_id using Cloudinary's URL generation.
        Additional transformation options can be passed via **options.
        """
        url, _ = cloudinary.utils.cloudinary_url(public_id, secure=True, **options)
        return url

    def scan_folder(self, folder_path: str, root_category: str,
                    skip_files: Optional[List[str]] = None) -> List[dict]:
        skip_files = skip_files or []
        expanded_folder_path, raw_folder_path = expand_or_preserve_env_vars(
                folder_path, None, keep_env_in_path=True)

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
        """
        Update the custom metadata field "display_name" for the given public_id.
        Must define "display_name" in Cloudinary as a custom structured metadata field.
        """
        try:
            result = cloudinary.api.update(public_id, display_name=display_name)
            print(f"Set display_name='{display_name}' on public_id={public_id}")
        except Exception as e:
            print(f"Failed to set display_name: {e}")


    def upload_assets(self, folder_path: str, root_category: str, 
                      skip_files: Optional[List[str]] = None) -> List[dict]:
        files_data = self.scan_folder(folder_path, root_category, skip_files)
        uploaded_files = []
        for file_info in files_data:
            response = self.upload_file(file_info, root_category)
            file_info["image_url"] = response["secure_url"]
            uploaded_files.append(file_info)
            print(f"Uploaded: {file_info['file_name']} → {response['secure_url']}")
        return uploaded_files

    def update_assets(
        self,
        folder_path: str,
        root_category: str,
        sync_backend: Optional[BaseSyncBackend],
        skip_files: Optional[List[str]] = None,
        update_tags: bool = True
    ):
        if not sync_backend:
            raise ValueError("No backend provided.")
    
        # Scan the folder for files.
        scanned_files = self.scan_folder(folder_path, root_category, skip_files)
        # Fetch existing entries from the backend.
        existing_entries = sync_backend.fetch_existing_entries()
        # Map scanned files by their hash.
        scanned_by_hash = {f["hash"]: f for f in scanned_files}
    
        # --- Process Additions and Updates ---
        for file_hash, file_info in scanned_by_hash.items():
            # Build a display name by replacing underscores with spaces and title-casing.
            display_name = Path(file_info["file_name"]).stem.replace("_", " ").title()
            file_info["display_name"] = display_name
            # Set the "path" field to the raw_path (source file path).
            file_info["path"] = file_info["raw_path"]
    
            if file_hash in existing_entries:
                # EXISTING FILE: file hash matches an existing record.
                existing_entry = existing_entries[file_hash]
    
                # For JSON backend, use stored "file_name" and "raw_path" directly.
                if isinstance(sync_backend, LocalJsonSyncBackend):
                    stored_file_name = existing_entry.get("file_name", "")
                    stored_path = existing_entry.get("raw_path", "")
                    scanned_path = file_info["raw_path"]
                else:
                    stored_file_name = Path(os.path.expandvars(existing_entry.get("path", ""))).name
                    stored_path = os.path.expandvars(existing_entry.get("path", ""))
                    scanned_path = os.path.expandvars(file_info["raw_path"])
    
                # Check if the file name has changed.
                if stored_file_name.lower() != file_info["file_name"].lower():
                    # RENAME: The file name has changed.
                    old_cloud_url = existing_entry.get("image_url", "")
                    old_public_id = self._extract_public_id(old_cloud_url)
                    new_public_id = f"{root_category}/{Path(file_info['file_name']).stem.lower()}"
                    try:
                        rename_resp = cloudinary.uploader.rename(old_public_id, new_public_id)
                        print(f"Renamed Cloudinary asset {old_public_id} -> {new_public_id}")
                        resource_info = cloudinary.api.resource(new_public_id)
                        new_url = resource_info["secure_url"]
                        file_info["image_url"] = create_new_url(new_url)
                    except Exception as e:
                        print("[CloudinaryManager] rename failed:", e)
                        # Fallback: re-upload the file.
                        rename_resp = self.upload_file(file_info, root_category)
                        new_url = rename_resp["secure_url"]
                        file_info["image_url"] = create_new_url(new_url)
                        new_public_id = rename_resp["public_id"]
    
                    self._update_display_name(new_public_id, display_name)
                    file_info["name"] = display_name  # Update Notion title.
                    sync_backend.update_entry(file_info, existing_entry)
                    # Update the in-memory entry.
                    if isinstance(sync_backend, LocalJsonSyncBackend):
                        existing_entry["raw_path"] = file_info["raw_path"]
                        existing_entry["file_name"] = file_info["file_name"]
                    else:
                        existing_entry["path"] = file_info["raw_path"]
    
                else:
                    # UPDATE: File name is the same; check if the source path or tags changed.
                    existing_tags = existing_entry.get("tags", [])
                    if isinstance(sync_backend, LocalJsonSyncBackend):
                        path_changed = (file_info["raw_path"] != existing_entry.get("raw_path", ""))
                    else:
                        stored_path = os.path.expandvars(existing_entry.get("path", ""))
                        scanned_path = os.path.expandvars(file_info["raw_path"])
                        path_changed = (scanned_path != stored_path)
                    tags_changed = (update_tags and existing_tags != file_info["tags"])
    
                    if path_changed or tags_changed:
                        reup_resp = self.upload_file(file_info, root_category)
                        new_url = reup_resp["secure_url"]
                        file_info["image_url"] = create_new_url(new_url)
                        self._update_display_name(reup_resp["public_id"], display_name)
                        file_info["name"] = display_name
                        sync_backend.update_entry(file_info, existing_entry)
                        print(f"[CloudinaryManager] Updated entry for {file_info['file_name']}")
                        if isinstance(sync_backend, LocalJsonSyncBackend):
                            existing_entry["raw_path"] = file_info["raw_path"]
                        else:
                            existing_entry["path"] = file_info["raw_path"]
                    else:
                        print(f"[CloudinaryManager] No change for {file_info['file_name']}")
    
            else:
                # NEW HASH: No matching entry by file hash.
                # Check if a file with the same name already exists (i.e., content changed).
                matching_entry = None
                for entry in existing_entries.values():
                    if isinstance(sync_backend, LocalJsonSyncBackend):
                        existing_name = entry.get("file_name", "")
                    else:
                        existing_name = Path(os.path.expandvars(entry.get("path", ""))).name.lower()
                    if (isinstance(sync_backend, LocalJsonSyncBackend) and existing_name == file_info["file_name"]) or \
                       (not isinstance(sync_backend, LocalJsonSyncBackend) and existing_name == file_info["file_name"].lower()):
                        matching_entry = entry
                        break
    
                if matching_entry:
                    # CONTENT CHANGED: Same name, but different (new) hash.
                    print(f"Content change detected for {file_info['file_name']}")
                    old_cloud_url = matching_entry.get("image_url", "")
                    old_public_id = self._extract_public_id(old_cloud_url)
                    try:
                        destroy_resp = cloudinary.uploader.destroy(old_public_id)
                        if destroy_resp.get("result") == "ok":
                            print(f"Deleted old Cloudinary asset {old_public_id}")
                        else:
                            print("Error deleting old asset:", destroy_resp)
                    except Exception as e:
                        print("Failed to delete old asset:", e)
    
                    reup_resp = self.upload_file(file_info, root_category)
                    new_url = reup_resp["secure_url"]
                    transformed_url = create_new_url(new_url)
                    file_info["image_url"] = transformed_url
                    file_info["cover"] = {"type": "external", "external": {"url": transformed_url}}
                    file_info["hash"] = file_hash
                    self._update_display_name(reup_resp["public_id"], display_name)
                    file_info["name"] = display_name
                    sync_backend.update_entry(file_info, matching_entry)
                    if isinstance(sync_backend, LocalJsonSyncBackend):
                        matching_entry["raw_path"] = file_info["raw_path"]
                        matching_entry["hash"] = file_info["hash"]
                    else:
                        matching_entry["path"] = file_info["raw_path"]
    
                else:
                    # NEW FILE: Completely new file.
                    upload_resp = self.upload_file(file_info, root_category)
                    original_url = upload_resp["secure_url"]
                    self._update_display_name(upload_resp["public_id"], display_name)
                    transformed_url = create_new_url(original_url)
                    file_info["image_url"] = transformed_url
                    file_info["cover"] = {"type": "external", "external": {"url": transformed_url}}
                    file_info["name"] = display_name
                    if isinstance(sync_backend, NotionSyncBackend):
                        default_icon = sync_backend.notion_db_config.default_icon or {}
                        if default_icon:
                            file_info["icon"] = default_icon
                    sync_backend.create_entry(file_info)
                    print(f"[CloudinaryManager] Created new entry for {file_info['file_name']}")
    
        # --- Process Deletions ---
        # Iterate over a copy of the items to avoid modifying the dictionary during iteration.
        for file_hash, existing_entry in list(existing_entries.items()):
            if isinstance(sync_backend, LocalJsonSyncBackend):
                stored_name = existing_entry.get("file_name", "").lower()
            else:
                stored_name = Path(os.path.expandvars(existing_entry.get("path", ""))).name.lower()
    
            found = False
            for f in scanned_files:
                if stored_name == f["file_name"].lower():
                    found = True
                    break
            if not found:
                old_cloud_url = existing_entry.get("image_url", "")
                public_id = self._extract_public_id(old_cloud_url)
                sync_backend.delete_entry(existing_entry)
                try:
                    destroy_resp = cloudinary.uploader.destroy(public_id)
                    if destroy_resp.get("result") == "ok":
                        print(f"[CloudinaryManager] Deleted Cloudinary asset {public_id}")
                    else:
                        print("[CloudinaryManager] Error deleting asset:", destroy_resp)
                except Exception as e:
                    print("[CloudinaryManager] Failed to delete asset:", e)
    
        print("[CloudinaryManager] Sync complete.")


# -------------------------------------------------------------------
# Main: Reading `sync_config.json` and Running Sync Jobs
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    def run_sync_jobs(sync_job_name=None, run_all=False):
        """
        Runs one or all sync jobs defined in the configuration.

        :param sync_job_name: Name of the sync job to run.
        :param run_all: If True, run all sync jobs regardless of sync_job_name.
        """
        # 1) Load the overall sync config
        config_data = load_sync_config()
        sync_jobs = config_data.get("sync_jobs", [])
        if not sync_jobs:
            print("No sync_jobs found in config.")
            return

        # 2) Initialize the CloudinaryManager once
        cloud_manager = CloudinaryManager()

        # 3) Determine which jobs to run: either all or filter by name
        if run_all:
            jobs_to_run = sync_jobs
        else:
            jobs_to_run = [job for job in sync_jobs if job.get("name") == sync_job_name]
            if not jobs_to_run:
                print(f"No sync job found with name: {sync_job_name}")
                return

        # 4) Iterate over selected jobs and run each one
        for job in jobs_to_run:
            job_name = job.get("name")
            folder_path = job.get("path")
            method = job.get("method", {})
            method_type = method.get("type")
            
            
            print(f"\n--- Running sync job '{job_name}' ---")

            if method_type == "notiondb":
                # Read notiondb config from method.notiondb
                notiondb_cfg = method.get("notiondb", {})
                notion_forward = method.get("forward_mapping", {})
                notion_reverse = method.get("reverse_mapping", {})



                # Create a NotionDBConfig
                db_id = notiondb_cfg.get("id")
                default_icon = notiondb_cfg.get("default_icon", {})

                # Load Notion API key from environment variable
                notion_api_key = os.getenv("NOTION_API_KEY", "YOUR_NOTION_API_KEY")

                notion_db_config = NotionDBConfig(
                    database_id=db_id,
                    forward_mapping=notion_forward,
                    back_mapping=notion_reverse,
                    default_icon=default_icon
                )

                # Create the Notion backend
                notion_backend = NotionSyncBackend(
                    notion_api_key=notion_api_key,
                    notion_db_config=notion_db_config
                )

                # Call update_assets for the Notion backend
                cloud_manager.update_assets(
                    folder_path=folder_path,
                    root_category=job_name,
                    sync_backend=notion_backend
                )

            elif method_type == "jsonlog":
                jsonlog_cfg = method.get("jsonlog", {})
                # Example configuration:
                # {
                #   "file_name": "sync_log.json",
                #   "in_folder": true,
                #   "log_path": ""
                # }

                log_file_name = jsonlog_cfg.get("file_name", "sync_log.json")
                in_folder = jsonlog_cfg.get("in_folder", True)
                log_path = jsonlog_cfg.get("log_path", "")

                if in_folder:
                    # Place the log in the same folder we are scanning
                    expanded_folder, _ = expand_or_preserve_env_vars(folder_path)
                    log_json_path = expanded_folder / log_file_name
                else:
                    # Or use the provided log_path
                    log_json_path = Path(log_path) / log_file_name if log_path else Path(log_file_name)

                json_backend = LocalJsonSyncBackend(str(log_json_path))

                # Call update_assets for the JSON log backend
                cloud_manager.update_assets(
                    folder_path=folder_path,
                    root_category=job_name,
                    sync_backend=json_backend
                )
            else:
                print(f"Unknown method type: {method_type}")
                continue

        print("[Main] Selected sync jobs completed.")

    # Setup argparse for CLI support
    parser = argparse.ArgumentParser(description="Run sync jobs individually or all together.")
    parser.add_argument("--job", help="Name of the sync job to run.")
    parser.add_argument("--all", action="store_true", help="Run all sync jobs.")
    args = parser.parse_args()

    # Execute the sync jobs based on CLI arguments
    run_sync_jobs(sync_job_name=args.job, run_all=args.all)
