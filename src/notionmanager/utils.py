import re
import json, csv
import pickle
import time
import shutil
from urllib.parse import urlparse


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
                print(f"✅ Found {len(page['databases'])} databases for page {page_id}")
            except Exception as e:
                print(f"❌ Failed to fetch databases for page {page_id}: {e}")

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

                print(f"✅ Found {len(database['pages'])} pages for database {database_id}")

            except Exception as e:
                print(f"❌ Failed to fetch pages for database {database_id}: {e}")

    # Save raw data to a pickle file
    save_pages_to_pickle(pickle_file_path, all_pages_data)

    # Write updated JSON back to file
    with open(output_file_path, "w") as file:
        json.dump(data, file, indent=4)

    print(f"📂 Updated JSON saved to {output_file_path}")

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

    move_unused_banner_files(cover_file_path, banner_folder, archive_folder)


