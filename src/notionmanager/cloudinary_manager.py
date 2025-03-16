import os
import re
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Tuple, Any

# Import helper functions from your utils module.
from notionmanager.utils import (
    expand_or_preserve_env_vars,
    compute_file_hash,
    generate_tags,
    create_new_url
)
from notionmanager.notion import NotionManager

class CloudinaryManager:
    def __init__(self, cloud_name: Optional[str] = None, api_key: Optional[str] = None, 
                 api_secret: Optional[str] = None, **config):
        """
        Initializes CloudinaryManager with credentials.
        Loads credentials from parameters or from .env (current directory or user home).
        """
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
        """
        Extracts the public_id from a Cloudinary URL, ignoring transformation parameters.
        For example:
          https://res.cloudinary.com/dicttuyma/image/upload/w_1500,h_600,c_fill,g_auto/v1742155960/banner/abstract_18.jpg
        returns: "banner/abstract_18"
        """
        try:
            pattern = r"/upload/(?:[^/]+/)?v\d+/([^\.]+)\."
            match = re.search(pattern, url)
            if match:
                return match.group(1)
            else:
                return ""
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
        response = cloudinary.uploader.upload(
            str(file_path),
            folder=f"{root_category}/",
            tags=file_info["tags"],
            use_filename=True,
            unique_filename=False
        )
        return response

    def upload_assets(self, folder_path: str, root_category: str, 
                      skip_files: Optional[List[str]] = None) -> List[dict]:
        files_data = self.scan_folder(folder_path, root_category, skip_files)
        uploaded_files = []
        for file_info in files_data:
            response = self.upload_file(file_info, root_category)
            file_info["cloudinary_url"] = response["secure_url"]
            uploaded_files.append(file_info)
            print(f"Uploaded: {file_info['file_name']} → {response['secure_url']}")
        return uploaded_files

    def _update_display_name(self, public_id: str, display_name: str):
        """
        Update the custom metadata field "display_name" for the given public_id.
        Must define "display_name" in Cloudinary as a custom structured metadata field.
        """
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
        notion_api_key: Optional[str] = None,
        notion_database_id: Optional[str] = None,
        update_tags: bool = True,
        skip_files: Optional[List[str]] = None
    ):
        if not notion_api_key or not notion_database_id:
            env_path = Path(__file__).parent / ".env"
            prod_env_path = Path.home() / ".notionmanager" / ".env"
            if env_path.exists():
                load_dotenv(env_path)
            elif prod_env_path.exists():
                load_dotenv(prod_env_path)
            notion_api_key = notion_api_key or os.getenv("NOTION_API_KEY")
            notion_database_id = notion_database_id or os.getenv("NOTION_DATABASE_ID")
            if not notion_api_key or not notion_database_id:
                raise ValueError("Notion API credentials missing.")

        notion_manager = NotionManager(notion_api_key, notion_database_id)

        properties_mapping = {
            "id": {"target": "id", "return": "str"},
            "icon": {"target": "icon", "return": "object"},
            "cover": {"target": "cover", "return": "object"},
            "Cover Name": {"target": "name", "type": "title", "return": "str"},
            "Image URL": {"target": "image_url", "type": "rich_text", "return": "str"},
            "Tags": {"target": "tags", "type": "multi_select", "return": "list"},
            "Source File Path": {"target": "path", "type": "rich_text", "return": "str"},
            "File Hash": {"target": "hash", "type": "rich_text", "return": "str"}
        }
        notion_pages_raw = notion_manager.get_pages()
        notion_pages = notion_manager.transform_pages(notion_pages_raw, properties_mapping)

        scanned_files = self.scan_folder(folder_path, root_category, skip_files)
        scanned_by_hash = {f["hash"]: f for f in scanned_files}
        notion_by_hash = {}
        for page in notion_pages:
            h = page.get("hash")
            if h:
                notion_by_hash[h] = page

        back_mapping = {
            "icon": {"target": "icon", "return": "object"},
            "cover": {"target": "cover", "return": "object"},
            "name": {"target": "Cover Name", "type": "title", "return": "str"},
            "image_url": {"target": "Image URL", "type": "rich_text", "return": "str", "property_id": "gS%7D%3C", "code": True},
            "tags": {"target": "Tags", "type": "multi_select", "return": "list", "property_id": "_G_%5D"},
            "path": {"target": "Source File Path", "type": "rich_text", "return": "str", "property_id": "uUkM", "code": True},
            "hash": {"target": "File Hash", "type": "rich_text", "return": "str", "property_id": "FJpK", "code": True}
        }

        default_icon = {
            "icon": {
                "type": "custom_emoji",
                "custom_emoji": {
                    "id": "1b7a1865-b187-8094-9473-007ae41f1605",
                    "name": "cover-image",
                    "url": "https://s3-us-west-2.amazonaws.com/public.notion-static.com/4fe77a7a-82e5-43e4-9cf8-18f00a46359d/cover-image.svg"
                }
            }
        }

        # --- Process additions, updates, and renames based on file hash ---
        for file_hash, file_info in scanned_by_hash.items():
            cover_name = Path(file_info["file_name"]).stem.title()

            if file_hash not in notion_by_hash:
                # New file
                response = self.upload_file(file_info, root_category)
                file_info["cloudinary_url"] = response["secure_url"]
                self._update_display_name(response["public_id"], cover_name)

                transformed_url = create_new_url(response["secure_url"])
                flat_object = {
                    "icon": default_icon["icon"],
                    "cover": {"type": "external", "external": {"url": transformed_url}},
                    "name": cover_name,
                    "image_url": transformed_url,
                    "tags": file_info["tags"],
                    "path": file_info["raw_path"],
                    "hash": file_info["hash"]
                }
                payload = notion_manager.build_notion_payload(flat_object, back_mapping)
                if flat_object.get("icon") and flat_object["icon"].get("type") == "custom_emoji":
                    payload["icon"] = flat_object["icon"]
                notion_manager.add_page(payload)
                print(f"Added new Notion page for {file_info['file_name']}")
            else:
                existing_page = notion_by_hash[file_hash]
                existing_raw_path = existing_page.get("path", "")
                existing_file_name = Path(os.path.expandvars(existing_raw_path)).name
                current_file_name = file_info["file_name"]

                if existing_file_name != current_file_name:
                    # rename
                    existing_image_url = existing_page.get("image_url", "")
                    old_public_id = self._extract_public_id(existing_image_url)
                    new_public_id = f"{root_category}/{current_file_name.rsplit('.', 1)[0]}"

                    try:
                        rename_response = cloudinary.uploader.rename(old_public_id, new_public_id)
                        print(f"Renamed Cloudinary asset from {old_public_id} to {new_public_id}")
                    except Exception as e:
                        print(f"Failed to rename Cloudinary asset: {e}")
                        rename_response = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = rename_response["secure_url"]
                        old_public_id = None
                        new_public_id = rename_response["public_id"]

                    self._update_display_name(new_public_id, cover_name)

                    try:
                        resource_info = cloudinary.api.resource(new_public_id)
                        new_secure_url = resource_info["secure_url"]
                    except Exception as e:
                        print(f"Failed to fetch resource info: {e}")
                        new_secure_url = file_info.get("cloudinary_url", "")

                    flat_object = {
                        "name": cover_name,
                        "image_url": create_new_url(new_secure_url),
                        "tags": file_info["tags"],
                        "path": file_info["raw_path"]
                    }
                    update_payload = notion_manager.build_notion_payload(flat_object, back_mapping)["properties"]
                    notion_manager.update_page(existing_page["id"], update_payload)
                    print(f"Updated Notion page for file rename: {current_file_name}")
                else:
                    # check if path/tags changed
                    if existing_raw_path != file_info["raw_path"] or (update_tags and existing_page.get("tags") != file_info["tags"]):
                        response = self.upload_file(file_info, root_category)
                        file_info["cloudinary_url"] = response["secure_url"]
                        self._update_display_name(response["public_id"], cover_name)

                        transformed_url = create_new_url(response["secure_url"])
                        flat_object = {
                            "name": cover_name,
                            "image_url": transformed_url,
                            "tags": file_info["tags"],
                            "path": file_info["raw_path"]
                        }
                        update_payload = notion_manager.build_notion_payload(flat_object, back_mapping)["properties"]
                        notion_manager.update_page(existing_page["id"], update_payload)
                        print(f"Updated Notion page metadata for {current_file_name}")
                    else:
                        print(f"No changes detected for {current_file_name}")

        # --- Handle deletion
        for file_hash, page in notion_by_hash.items():
            if file_hash not in scanned_by_hash:
                # File not found on disk => delete from Notion and Cloudinary
                # Use the public_id extracted from page["image_url"] rather than local file name
                existing_image_url = page.get("image_url", "")
                public_id = self._extract_public_id(existing_image_url)

                try:
                    # Delete from Notion (archive)
                    notion_manager.delete_page(page["id"])

                    # Delete from Cloudinary
                    destroy_resp = cloudinary.uploader.destroy(public_id)
                    if destroy_resp.get("result") == "ok":
                        print(f"Deleted Cloudinary asset for {public_id}")
                    else:
                        print(f"Error deleting Cloudinary asset for {public_id}: {destroy_resp}")
                    print(f"Deleted Notion page for {public_id}")
                except Exception as e:
                    print(f"Failed to delete asset for {public_id}: {e}")

        print("Synchronization complete.")


# Example usage:
if __name__ == "__main__":
    manager = CloudinaryManager()
    manager.update_assets(
        folder_path="$DROPBOX/pictures/assets/banner",
        root_category="banner"
    )
