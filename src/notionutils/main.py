from oauthmanager import AuthManager, OnePasswordAuthManager
from notion import NotionManager

def main():
    # Initialize the AuthManager
    auth_manager = OnePasswordAuthManager()

    # Retrieve credentials for Notion
    notion_creds = auth_manager.get_credentials("Notion", "credential")

    NOTION_API_KEY = notion_creds['credential']
    DATABASE_ID = "13ca1865b187803390f4d1b6b0bd92e8"

    # Cover image URL (broken down for better readability)
    new_cover_url = (
        "https://images.unsplash.com/photo-1690321607902-2799a1e8eaaa"
        "?q=80&w=3540&auto=format&fit=crop&ixlib=rb-4.0.3&ixid="
        "M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
    )

    # Create an instance of NotionManager and update the cover image
    install_db = NotionManager(NOTION_API_KEY, DATABASE_ID)
    install_db.update_cover_image(cover_url=new_cover_url)

if __name__ == "__main__":
    main()
