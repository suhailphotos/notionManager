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

def remove_links_from_filenames():
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

# Run the function
remove_links_from_filenames()
