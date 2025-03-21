import os
import sys
import ctypes
import re
import hashlib
import json, csv
import pickle
import time
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Any

def expand_or_preserve_env_vars(
    raw_path: Optional[str],
    parent_path: Optional[Any] = None,
    keep_env_in_path: bool = True
) -> Tuple[Path, str]:
    """
    Takes a potential 'raw_path' that may contain environment variables like '$ANYVAR/some/dir'.
    
    1) If 'raw_path' is provided, we fully expand it using os.path.expandvars for disk usage.
    2) Meanwhile, final_path_str retains the original (with $VAR) if keep_env_in_path is True.
    3) If raw_path is None, we fallback to parent_path (processed similarly) or to ~/Documents.
    
    Returns (expanded_path, final_path_str).
    """
    if raw_path:
        final_path_str = raw_path if keep_env_in_path else os.path.expandvars(raw_path)
        expanded = os.path.expandvars(raw_path)
        expanded_path = Path(expanded).expanduser()
        return expanded_path, final_path_str
    else:
        if parent_path is not None:
            if isinstance(parent_path, str):
                final_path_str = parent_path if keep_env_in_path else os.path.expandvars(parent_path)
                expanded = os.path.expandvars(parent_path)
                expanded_path = Path(expanded).expanduser()
                return expanded_path, final_path_str
            else:
                return parent_path, str(parent_path)
        default_fallback = Path.home() / "Documents"
        return default_fallback, str(default_fallback)

def compute_file_hash(file_path: Path) -> str:
    """Compute MD5 hash for a given file."""
    hasher = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def generate_tags(relative_path: Path, root_category: str) -> List[str]:
    """
    Generates tags based on the folder hierarchy.
    For example, for file '/path/to/banner/programming/matplotlib.jpg'
    returns ['banner', 'programming'].
    """
    tag_list = [root_category] + list(relative_path.parts[:-1])
    filtered_tags = []
    for tag in tag_list:
        t = tag.lower().replace(" ", "_")
        if t not in filtered_tags:
            filtered_tags.append(t)
    return filtered_tags


def extract_id_from_url(notion_url):
    """
    Extracts the database or page ID from a Notion URL and formats it as a UUID.
    """
    match = re.search(r"([a-f0-9]{32})", notion_url)
    if not match:
        raise ValueError("Invalid Notion URL: No valid ID found.")
    
    raw_id = match.group(1)
    return format_uuid(raw_id)



def format_uuid(raw_id):
    """
    Converts a 32-character Notion ID into UUID format.
    Example: "175a1865b1878060a675d400cffc6268" -> "175a1865-b187-8060-a675-d400cffc6268"
    """
    return f"{raw_id[:8]}-{raw_id[8:12]}-{raw_id[12:16]}-{raw_id[16:20]}-{raw_id[20:]}"


def update_notion_ids(json_file_path):
    """
    Reads a JSON file, extracts the Notion page IDs from URLs, updates the "id" field,
    and writes the updated JSON back to the file.
    """
    # Read JSON file
    with open(json_file_path, "r") as file:
        data = json.load(file)

    # Iterate through parent_page list and update IDs
    for page in data.get("parent_page", []):
        if page.get("url") and page.get("id") is None:
            try:
                page["id"] = extract_id_from_url(page["url"])
            except ValueError as e:
                print(f"Skipping invalid URL: {page['url']} - {e}")

    # Write updated JSON back to file
    with open(json_file_path, "w") as file:
        json.dump(data, file, indent=4)

    print(f"Updated Notion IDs in {json_file_path}")




def remove_links_from_filenames(notion, database_id):
    """Removes links from filenames in the specified Notion database."""
    # Query all pages in the database
    results = notion.databases.query(database_id=database_id)["results"]

    for page in results:
        # Extract the page ID and properties
        page_id = page["id"]
        properties = page["properties"]

        # Check if the "Filename" property contains a link
        filename_property = properties.get("Filename")
        if filename_property and filename_property["type"] == "title":
            # Extract only the text content, discarding any links
            updated_filename = [
                {"text": {"content": item["text"]["content"]}}
                for item in filename_property["title"]
            ]

            # Update the page to remove links from the "Filename" property
            notion.pages.update(
                page_id=page_id,
                properties={
                    "Filename": {
                        "type": "title",
                        "title": updated_filename
                    }
                }
            )
            print(f"Updated filename for page {page_id}")

def read_csv(csv_path):
    """Reads a CSV file and maps filenames to full paths."""
    file_data = {}
    with open(csv_path, "r") as file:
        next(file)  # Skip the header
        for line in file:
            filename, full_path = line.strip().split(",", 1)
            file_data[filename] = full_path
    return file_data


def read_file_content(file_path):
    """Reads the content of a file."""
    with open(file_path, "r") as file:
        return file.read()


def get_databases_for_pages(notion, json_file_path):
    """
    Queries Notion API for all database-type objects under each page, including those nested
    inside callout blocks, toggles, columns, and synced blocks.
    Updates the JSON file by adding them to the 'databases' list.
    """
    def fetch_databases_recursive(parent_id):
        """Recursively fetch databases from a Notion page or block, handling nested structures."""
        databases = []
        next_cursor = None

        while True:
            response = notion.blocks.children.list(block_id=parent_id, start_cursor=next_cursor)

            for item in response["results"]:
                # If block is a database, add it to the list
                if item["type"] == "child_database":
                    databases.append({
                        "id": item["id"],
                        "name": item["child_database"].get("title", "Unnamed Database")
                    })

                # If block is a container, recursively search inside it
                elif item["type"] in ["toggle", "column_list", "column", "callout", "synced_block"]:
                    print(f"🔍 Searching inside nested block {item['type']} ({item['id']})...")
                    databases.extend(fetch_databases_recursive(item["id"]))

            # Handle pagination
            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break

        return databases

    # Read JSON file
    with open(json_file_path, "r") as file:
        data = json.load(file)

    for page in data.get("parent_page", []):
        page_id = page.get("id")
        if page_id:
            try:
                print(f"📄 Fetching databases for page {page_id}...")
                page["databases"] = fetch_databases_recursive(page_id)
                print(f"Found {len(page['databases'])} databases for page {page_id}")
            except Exception as e:
                print(f"Failed to fetch databases for page {page_id}: {e}")

    # Write updated JSON back to file
    with open(json_file_path, "w") as file:
        json.dump(data, file, indent=4)

    print(f"📂 Updated Notion database list in {json_file_path}")


def insert_code_to_notion(notion, database_id, csv_file_path):
    """Inserts code content as a code block into Notion pages."""
    file_data = read_csv(csv_file_path)
    results = notion.databases.query(database_id=database_id)["results"]

    for page in results:
        page_id = page["id"]
        properties = page["properties"]

        filename_property = properties.get("Filename")
        if filename_property and filename_property["type"] == "title":
            filename = filename_property["title"][0]["text"]["content"]

            if filename in file_data:
                file_path = file_data[filename]
                code_content = read_file_content(file_path)

                # Insert code content as a code block in the Notion page
                notion.blocks.children.append(
                    page_id,
                    children=[
                        {
                            "object": "block",
                            "type": "code",
                            "code": {
                                "rich_text": [{"type": "text", "text": {"content": code_content}}],
                                "language": "python"
                            }
                        }
                    ]
                )
                print(f"Inserted code content into page: {filename}")

def fetch_all_pages(notion, database_id, save_interval=10):
    """
    Queries Notion API to retrieve all pages inside a database.
    Handles pagination, rate limits, and incremental saving.
    """
    pages = []
    next_cursor = None
    request_count = 0

    while True:
        try:
            response = notion.databases.query(database_id=database_id, start_cursor=next_cursor, page_size=100)
            pages.extend(response["results"])
            request_count += 1

            # Save intermediate results every `save_interval` requests
            if request_count % save_interval == 0:
                print(f"💾 Saving progress after {request_count} API requests...")
                save_pages_to_pickle("notion_pages_temp.pkl", pages)

            # Handle pagination
            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
                time.sleep(0.5)  # 💤 Prevent hitting rate limits
            else:
                break  # Exit loop when all pages are retrieved

        except Exception as e:
            print(f"❌ Error fetching pages: {e}")
            break

    return pages


def save_pages_to_pickle(file_path, data):
    """Saves Notion page data incrementally to a pickle file."""
    with open(file_path, "wb") as file:
        pickle.dump(data, file)
    print(f"✅ Saved Notion pages to {file_path}")


def update_json_with_pages(notion, json_file_path, output_file_path, pickle_file_path):
    """
    Fetches all pages for each database, handles rate limits,
    saves full data to a pickle file, and updates JSON structure.
    """
    # Read JSON file
    with open(json_file_path, "r") as file:
        data = json.load(file)

    all_pages_data = {}  # Store raw page data

    for page in data.get("parent_page", []):
        for database in page.get("databases", []):
            database_id = database["id"]

            try:
                print(f"📦 Fetching pages for database: {database['name']} ({database_id})...")
                pages = fetch_all_pages(notion, database_id)
                
                # Store raw page data for future reference
                all_pages_data[database_id] = pages  

                # Extract only page_id and cover for JSON
                database["pages"] = [
                    {
                        "page_id": page["id"],
                        "cover": page.get("cover", {}).get("external", {}).get("url") if page.get("cover") else None
                    }
                    for page in pages
                ]

                print(f"Found {len(database['pages'])} pages for database {database_id}")

            except Exception as e:
                print(f"Failed to fetch pages for database {database_id}: {e}")

    # Save raw data to a pickle file
    save_pages_to_pickle(pickle_file_path, all_pages_data)

    # Write updated JSON back to file
    with open(output_file_path, "w") as file:
        json.dump(data, file, indent=4)

    print(f"📂 Updated JSON saved to {output_file_path}")

def create_new_url(cloudinary_url, transformation="w_1500,h_600,c_fill,g_auto"):
    """
    Given a Cloudinary URL, inserts the transformation parameters
    right after the 'upload/' marker.
    
    Example:
      Input:  https://res.cloudinary.com/dicttuyma/image/upload/v1742004118/banner/mariadb.jpg
      Output: https://res.cloudinary.com/dicttuyma/image/upload/w_1500,h_600,c_fill,g_auto/v1742004118/banner/mariadb.jpg
    """
    marker = "upload/"
    idx = cloudinary_url.find(marker)
    if idx == -1:
        # Marker not found, return the original URL as fallback
        return cloudinary_url
    idx += len(marker)
    return cloudinary_url[:idx] + transformation + "/" + cloudinary_url[idx:]

def extract_unique_covers(json_file_path, csv_output_path, json_output_path):
    """
    Extracts unique cover image URLs from the updated Notion JSON file,
    and generates a CSV and JSON file listing them.
    """
    # Read JSON file
    with open(json_file_path, "r") as file:
        data = json.load(file)

    unique_covers = {}

    for page_entry in data.get("parent_page", []):
        for database in page_entry.get("databases", []):
            for page in database.get("pages", []):
                cover_url = page.get("cover")
                
                if cover_url:
                    # Extract the file name from the URL
                    parsed_url = urlparse(cover_url)
                    file_name = os.path.basename(parsed_url.path)

                    # If URL is new, add it to the dictionary
                    if cover_url not in unique_covers:
                        unique_covers[cover_url] = {
                            "file_name": file_name,
                            "current_url": cover_url,
                            "new_url": None,  # Placeholder for Cloudinary URL
                            "num_pages": 1
                        }
                    else:
                        unique_covers[cover_url]["num_pages"] += 1

    # Convert dictionary to a sorted list of dictionaries
    cover_list = sorted(unique_covers.values(), key=lambda x: x["num_pages"], reverse=True)

    # Save to JSON file
    with open(json_output_path, "w") as json_file:
        json.dump({"cover": cover_list}, json_file, indent=4)

    # Save to CSV file
    with open(csv_output_path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["file_name", "current_url", "new_url", "num_pages"])
        writer.writeheader()
        writer.writerows(cover_list)

    print(f"✅ Cover images saved to:\n  - CSV: {csv_output_path}\n  - JSON: {json_output_path}")

def move_unused_banner_files(json_file_path, banner_folder, archive_folder):
    """
    Compares files in `banner_folder` against those listed in `cover_images.json`
    and moves unused files to `archive_folder`.
    
    Parameters:
    - json_file_path (str): Path to `cover_images.json`
    - banner_folder (str): Path to the folder containing banner images.
    - archive_folder (str): Path to move unused banner images.
    """
    # Read the cover images JSON file
    with open(json_file_path, "r") as file:
        cover_data = json.load(file)

    # Extract all file names that are actively used
    used_files = {item["file_name"] for item in cover_data.get("cover", [])}

    # Ensure archive folder exists
    os.makedirs(archive_folder, exist_ok=True)

    # List all files in the banner folder
    all_files = set(os.listdir(banner_folder))

    # Find unused files (files not listed in `cover_images.json`)
    unused_files = all_files - used_files

    if not unused_files:
        print("✅ No unused banner files found. Everything is in use.")
        return

    # Move unused files to the archive folder
    for file_name in unused_files:
        source_path = os.path.join(banner_folder, file_name)
        destination_path = os.path.join(archive_folder, file_name)

        try:
            shutil.move(source_path, destination_path)
            print(f"📂 Moved: {file_name} → {archive_folder}")
        except Exception as e:
            print(f"❌ Failed to move {file_name}: {e}")

    print(f"✅ Completed! {len(unused_files)} unused files moved to {archive_folder}.")

def hide_file(file_path: Path):
    """
    Hide file using OS-specific methods.
    For Windows, sets the hidden attribute.
    For macOS, uses 'chflags hidden'.
    On Linux, there's no reliable programmatic method to hide a file
    without renaming it (files are hidden only if their names begin with a dot).
    """
    if os.name == 'nt':  # Windows
        FILE_ATTRIBUTE_HIDDEN = 0x02
        ret = ctypes.windll.kernel32.SetFileAttributesW(str(file_path), FILE_ATTRIBUTE_HIDDEN)
        if not ret:
            print(f"Failed to hide file {file_path} on Windows")
    elif sys.platform == 'darwin':  # macOS
        # Use 'chflags hidden' to mark the file as hidden on macOS
        try:
            subprocess.run(['chflags', 'hidden', str(file_path)], check=True)
        except subprocess.CalledProcessError:
            print(f"Failed to hide file {file_path} on macOS")
    else:
        # On Linux, files are hidden only if they start with a dot.
        # If you need the same filename, there's no standard method to hide it.
        print("On Linux, a file is only hidden if its name begins with a dot. "
              "No cross-platform programmatic method is available without renaming.")


# ===================== TEST SECTION =====================

if __name__ == '__main__':
    import os
    from notion_client import Client
    from oauthmanager import OnePasswordAuthManager
    
    # Initialize the AuthManager
    auth_manager = OnePasswordAuthManager()

    # Retrieve Notion credentials from 1Password
    notion_creds = auth_manager.get_credentials("Quantum", "credential")

    # Initialize Notion client
    notion = Client(auth=notion_creds['credential'])

    # Replace with your actual database ID
    database_id = "143a1865b187807e8f7dd40cf1fef430"

    # Path to the CSV file generated from your script
    csv_file_path = "/Users/suhail/Desktop/python_files1.csv"

    # Path to the JSON file
    json_file_path = os.path.join(os.getcwd(), "notiondb_pages.json")

    # Path to the JSON file
    updated_json_file_path = os.path.join(os.getcwd(), "updated_notiondb_pages.json")

    output_file_path = os.path.join(os.getcwd(), "updated_notiondb_pages.json")  # Updated JSON
    pickle_file_path = os.path.join(os.getcwd(), "notion_pages.pkl")  # Pickle file to store raw pages

    csv_output_path = os.path.join(os.getcwd(), "cover_images.csv")  # Output CSV
    json_output_path = os.path.join(os.getcwd(), "cover_images.json")  # Output JSON

    cover_file_path = os.path.join(os.getcwd(), "cover_images.json")
    banner_folder = "/Users/suhail/Library/CloudStorage/Dropbox/matrix/packages/notionUtils/assets/media/banner"
    archive_folder = "/Users/suhail/Desktop/banners"

    

    # Test remove_links_from_filenames
    def test_remove_links():
        # Replace with your actual database ID
        database_id = "143a1865b187807e8f7dd40cf1fef430"

        remove_links_from_filenames(notion, database_id)


    # test_remove_links()

    # Test insert_code_to_notion
    # test_insert_code()
    def test_insert_code():
        # Path to the CSV file generated from your script
        csv_file_path = "/Users/suhail/Desktop/python_files1.csv"

        insert_code_to_notion(notion, database_id, csv_file_path)

    # Test update notion ids
    # update_notion_ids(json_file_path)

    # Uncomment to test
    # test_update_notion_id()


        
    # test_get_databases_for_pages()
    # get_databases_for_pages(notion, json_file_path)


    # Run the function to update the JSON with pages
    # update_json_with_pages(notion, json_file_path, output_file_path, pickle_file_path)

    # extract_unique_covers(updated_json_file_path, csv_output_path, json_output_path)

    # move_unused_banner_files(cover_file_path, banner_folder, archive_folder)


    def merge_cover_data(upload_results_path, cover_file_name_path, output_path):
        """
        Merges the cloudinary_url and tags from `upload_results.json` into
        the objects in `cover_file_name.json`, matching on `original_filename`
        == `new_file_name`.
    
        Args:
            upload_results_path (str or Path): Path to upload_results.json
            cover_file_name_path (str or Path): Path to cover_file_name.json
            output_path (str or Path): Where to write the merged JSON
        """
        upload_results_path = Path(upload_results_path)
        cover_file_name_path = Path(cover_file_name_path)
        output_path = Path(output_path)
    
        # Load upload_results.json
        with upload_results_path.open("r") as f:
            upload_results = json.load(f)
    
        # Create a dictionary keyed by original_filename
        # e.g. {"abstract_21.jpeg": {"original_filename": "...", "cloudinary_url": "...", "tags": [...]}, ...}
        upload_dict = {item["original_filename"]: item for item in upload_results}
    
        # Load cover_file_name.json
        with cover_file_name_path.open("r") as f:
            cover_data = json.load(f)
    
        # Iterate over each file entry in cover_file_name.json
        for entry in cover_data["files"]:
            new_name = entry["new_file_name"]
    
            # If there's a match, add cloudinary_url and tags
            if new_name in upload_dict:
                entry["cloudinary_url"] = upload_dict[new_name]["cloudinary_url"]
                entry["tags"] = upload_dict[new_name]["tags"]
    
        # Write the merged data to output_path
        with output_path.open("w") as f:
            json.dump(cover_data, f, indent=4)
    
        print(f"Merged data written to {output_path}")
    
    # Example usage:
    #merge_cover_data(
    #    "upload_results.json",
    #    "cover_file_name.json",
    #    "cover_file_name_merged.json"
    #)




    
    def merge_cover_images(cover_images_path, cover_file_name_merged_path, output_path):
        """
        Merges data from cover_images.json and cover_file_name_merged.json.
        
        For each entry in cover_images.json (keyed by "file_name"), if a match is found in
        cover_file_name_merged.json (using "old_file_name"), then add:
          - new_file_name
          - cloudinary_url
          - tags
          - new_url (created by applying Cloudinary transformations)
        
        The merged data is written to output_path.
        """
        cover_images_path = Path(cover_images_path)
        cover_file_name_merged_path = Path(cover_file_name_merged_path)
        output_path = Path(output_path)
        
        # Load cover_images.json
        with cover_images_path.open("r") as f:
            cover_images_data = json.load(f)
        
        # Load cover_file_name_merged.json
        with cover_file_name_merged_path.open("r") as f:
            cover_file_data = json.load(f)
        
        # Build a lookup dictionary using the common key (old_file_name)
        lookup = {item["old_file_name"]: item for item in cover_file_data.get("files", [])}
        
        # Process each cover image entry
        for cover in cover_images_data.get("cover", []):
            file_name = cover.get("file_name")
            if file_name in lookup:
                merged_info = lookup[file_name]
                # Add new_file_name, cloudinary_url, and tags from cover_file_name_merged.json
                cover["new_file_name"] = merged_info["new_file_name"]
                cover["cloudinary_url"] = merged_info["cloudinary_url"]
                cover["tags"] = merged_info["tags"]
                # Generate a new_url with the transformation parameters
                cover["new_url"] = create_new_url(merged_info["cloudinary_url"])
        
        # Write the merged output to a file
        with output_path.open("w") as f:
            json.dump(cover_images_data, f, indent=4)
        
        print(f"Merged cover images written to {output_path}")
    
#    merge_cover_images(
#        "cover_images.json",             # Path to your cover_images.json
#        "cover_file_name_merged.json",     # Path to your cover_file_name_merged.json
#        "cover_images_merged.json"         # Output path for the merged JSON
#    )

    log_json_path = Path("/Users/suhail/Library/CloudStorage/Dropbox/pictures/assets/icon/sync_log.json")
    hide_file(log_json_path)
