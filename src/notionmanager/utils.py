import re

def extract_database_id_from_url(database_url):
    """Extract the database ID from a Notion database URL."""
    match = re.search(r"([a-f0-9]{32})", database_url)
    if not match:
        raise ValueError("Invalid Notion database URL.")
    return match.group(1)


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


# ===================== TEST SECTION =====================

if __name__ == '__main__':
    import os
    from notion_client import Client
    from oauthmanager import OnePasswordAuthManager

    # Initialize the AuthManager
    auth_manager = OnePasswordAuthManager()

    # Retrieve Notion credentials from 1Password
    notion_creds = auth_manager.get_credentials("Notion", "credential")

    # Initialize Notion client
    notion = Client(auth=notion_creds['credential'])

    # Replace with your actual database ID
    database_id = "143a1865b187807e8f7dd40cf1fef430"

    # Path to the CSV file generated from your script
    csv_file_path = "/Users/suhail/Desktop/python_files1.csv"

    # Uncomment the function you want to test

    # Test remove_links_from_filenames
    # test_remove_links()
    def test_remove_links():
        # Replace with your actual database ID
        database_id = "143a1865b187807e8f7dd40cf1fef430"

        remove_links_from_filenames(notion, database_id)

    # Test insert_code_to_notion
    # test_insert_code()
    def test_insert_code():
        # Path to the CSV file generated from your script
        csv_file_path = "/Users/suhail/Desktop/python_files1.csv"

        insert_code_to_notion(notion, database_id, csv_file_path)
