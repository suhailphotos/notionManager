import re
from oauthmanager import AuthManager, OnePasswordAuthManager
from notion import NotionManager

def main():
    # Prompt the user for the Notion database URL and image URL
    database_url = input("Please enter the Notion database URL: ")
    image_url = input("Please enter the cover image URL: ")

    # Use regex to extract the DATABASE_ID from the URL
    match = re.search(r"\/([a-f0-9]{32})\?", database_url)
    if not match:
        print("Invalid Notion database URL provided. Please try again.")
        return
    DATABASE_ID = match.group(1)

    # Initialize the AuthManager
    auth_manager = OnePasswordAuthManager()

    # Retrieve credentials for Notion
    notion_creds = auth_manager.get_credentials("Notion", "credential")
    NOTION_API_KEY = notion_creds['credential']

    # Define the filter to only get entries where "Fresh macOS Install" is non-empty
    filter_condition = {
        "property": "Fresh macOS Install",
        "number": {
            "is_not_empty": True
        }
    }

    # Create an instance of NotionManager with the filter passed as kwargs
    install_db = NotionManager(NOTION_API_KEY, DATABASE_ID, filter=filter_condition)
    install_db.update_cover_image(cover_url=image_url)

if __name__ == "__main__":
    main()
