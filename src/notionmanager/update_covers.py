import os
import json
import random
from pathlib import Path
from dotenv import load_dotenv
from notionmanager.notion import NotionManager
from notionmanager.api import NotionAPI

def load_json(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)

def save_json(filepath: str, data: dict):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def get_cover_images(notion_api_key: str, cover_db_id: str) -> list:
    """
    Retrieves cover images from the Notion Cover Images database and transforms them.
    """
    nm = NotionManager(notion_api_key, cover_db_id)
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
    pages = nm.get_pages()
    transformed = nm.transform_pages(pages, properties_mapping)
    return transformed

def update_cover_names(cover_file_name_path: str, cover_names_path: str) -> dict:
    """
    Updates cover_names.json:
      - Loads cover_file_name.json to obtain a mapping (old_file_name -> new_file_name).
      - Loads cover_names.json, which lists cover entries (each with keys "file_name", "current_url", "new_url", etc.).
      - Uses the Cover Images database (via get_cover_images) to build a mapping 
        from new file name to new Cloudinary URL.
      - Updates each cover entry's "new_url" field accordingly.
      - Also, if a cover entry has no "tags" field, and if the new file name includes "notion",
        a default tag ["notion"] is added.
    """
    cover_file_data = load_json(cover_file_name_path)
    cover_names = load_json(cover_names_path)
    file_mapping = {entry["old_file_name"]: entry["new_file_name"] for entry in cover_file_data.get("files", [])}
    
    new_url_mapping = {}
    notion_api_key = os.getenv("NOTION_API_KEY")
    cover_db_id = os.getenv("NOTION_COVER_DATABASE_ID")  # e.g., "154a1865-b187-8082-9bd2-c4349fb0c736"
    if notion_api_key and cover_db_id:
        cover_pages = get_cover_images(notion_api_key, cover_db_id)
        for page in cover_pages:
            image_url = page.get("image_url", "")
            if image_url:
                file_name = Path(image_url.split("/")[-1]).name  # e.g., "json.jpg"
                new_url_mapping[file_name] = image_url
    else:
        print("Notion credentials for Cover Images database not provided; cannot build new URL mapping.")
    
    for cover in cover_names.get("cover", []):
        old_file = cover["file_name"]
        new_file = file_mapping.get(old_file)
        # Append default tags if not present.
        if "tags" not in cover:
            # If new_file contains "notion" (case-insensitive), add tag "notion"
            if new_file and "notion" in new_file.lower():
                cover["tags"] = ["notion"]
            else:
                cover["tags"] = []
        if new_file:
            new_url = new_url_mapping.get(new_file)
            if new_url:
                cover["new_url"] = new_url
                print(f"Updated mapping for {old_file} -> {new_file} with URL: {new_url}")
            else:
                print(f"New URL not found for new file name {new_file}")
        else:
            print(f"No mapping found for old file name {old_file}")
    
    save_json(cover_names_path, cover_names)
    return cover_names

def update_notion_covers(notion_api_key: str, notion_db_pages_path: str, cover_names: dict):
    """
    Updates Notion pages (from notion_db_pages.json) that reference old GitHub cover URLs,
    replacing them with new Cloudinary URLs.
    
    For each page:
      - Iterates over each parent → each database → each page.
      - If the cover field is None or the URL does not contain the expected GitHub pattern,
        the page is skipped.
      - Otherwise, extracts the file name from the cover URL.
      - Looks up the new URL in the updated cover_names mapping.
      - If not found, randomly assigns one from covers tagged "notion" (or a constant fallback).
      - Patches the page using NotionAPI.update_page().
    """
    pages_data = load_json(notion_db_pages_path)
    api = NotionAPI(notion_api_key)
    
    # Build a mapping from file name to new URL.
    cover_mapping = {entry["file_name"]: entry.get("new_url") for entry in cover_names.get("cover", [])}
    # Build fallback list: use covers that have the "notion" tag.
    fallback_urls = [entry.get("new_url") for entry in cover_names.get("cover", [])
                     if entry.get("tags") and any(t.lower() == "notion" for t in entry["tags"]) and entry.get("new_url")]
    # If fallback list is still empty, use a constant fallback.
    FALLBACK_COVER_URL = "https://res.cloudinary.com/dicttuyma/image/upload/w_1500,h_600,c_fill,g_auto/v1742094839/banner/notion_01.jpg"
    if not fallback_urls:
        fallback_urls = [FALLBACK_COVER_URL]
    
    # Iterate over the hierarchy: parent_page -> databases -> pages.
    for parent in pages_data.get("parent_page", []):
        for database in parent.get("databases", []):
            for page in database.get("pages", []):
                current_cover = page.get("cover")
                if not current_cover:
                    print(f"Page {page['page_id']} has no cover; skipping.")
                    continue
                # Expecting old GitHub URLs.
                expected_pattern = "github.com/suhailphotos/notionUtils/blob/main/assets/media/banner/"
                if expected_pattern not in current_cover:
                    print(f"Page {page['page_id']} cover is not an old GitHub URL; skipping.")
                    continue
                file_name = Path(current_cover.split("/")[-1].split("?")[0]).name
                new_url = cover_mapping.get(file_name)
                if not new_url:
                    new_url = random.choice(fallback_urls)
                    print(f"Randomly assigned fallback URL for {file_name}: {new_url}")
                if new_url:
                    update_payload = {
                        "cover": {
                            "type": "external",
                            "external": {
                                "url": new_url
                            }
                        }
                    }
                    try:
                        api.update_page(page["page_id"], update_payload)
                        print(f"Updated Notion page {page['page_id']} cover to: {new_url}")
                    except Exception as e:
                        print(f"Failed to update page {page['page_id']}: {e}")
                else:
                    print(f"No new URL found for file {file_name} and no fallback available.")
    print("Notion cover pages update complete.")

if __name__ == "__main__":
    import random
    from dotenv import load_dotenv
    
    # Load environment variables.
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    # File paths (adjust as needed)
    cover_file_name_path = "cover_file_name.json"   # Mapping of old file names to new file names.
    cover_names_path = "cover_images.json"           # JSON file listing cover entries (must include keys: file_name, current_url, new_url, and optionally tags).
    notion_db_pages_path = "notion_db_pages_copy.json"     # JSON file listing workspace pages with cover URLs.
    
    notion_api_key = os.getenv("NOTION_API_KEY")
    
    # Step 1: (Optional) Pull cover images from the Cover Images database.
    cover_images = get_cover_images(notion_api_key, os.getenv("NOTION_COVER_DATABASE_ID"))
    print(f"Retrieved {len(cover_images)} cover images from Notion Cover Images database.")
    
    # Step 2: Update cover_names.json using the mapping.
    updated_cover_names = update_cover_names(cover_file_name_path, cover_names_path)
    
    # Step 3: Update all Notion pages (from notion_db_pages.json) to use the new cover URLs.
    update_notion_covers(notion_api_key, notion_db_pages_path, updated_cover_names)
