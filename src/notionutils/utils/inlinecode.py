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

def convert_full_path_to_inline_code():
    # Query all pages in the database
    results = notion.databases.query(database_id=database_id)["results"]

    for page in results:
        # Extract the page ID and properties
        page_id = page["id"]
        properties = page["properties"]

        # Check if the "Full Path" property exists
        full_path_property = properties.get("Full Path")
        if full_path_property and full_path_property["type"] == "rich_text":
            # Extract the text content and wrap it in backticks
            updated_full_path = [
                {"text": {"content": f"`{item['text']['content']}`"}}
                for item in full_path_property["rich_text"]
            ]

            # Update the page to use inline code formatting
            notion.pages.update(
                page_id=page_id,
                properties={
                    "Full Path": {
                        "type": "rich_text",
                        "rich_text": updated_full_path
                    }
                }
            )
            print(f"Updated Full Path for page {page_id}")

# Run the function
convert_full_path_to_inline_code()
