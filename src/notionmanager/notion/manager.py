from notionmanager.notion.api import NotionAPI

class NotionManager:
    def __init__(self, api_key, database_id):
        """
        Initialize the NotionManager with API key and database ID.

        Parameters:
        - api_key (str): Notion API key.
        - database_id (str): ID of the database to manage.
        """
        self.api = NotionAPI(api_key)  # Authentication is fully handled by NotionAPI
        self.database_id = database_id
        self.title_property_name = None  # Loaded only when needed

    def get_page(self, page_id):
        """
        Fetches a single Notion page by its ID.

        Parameters:
        - page_id (str): The Notion page ID.

        Returns:
        - dict: Page details if found.
        """
        return self.api.get_page(page_id)

    def get_pages(self, num_pages=None, retrieve_all=False, **kwargs):
        """
        Fetch pages from the database with optional pagination.

        Parameters:
        - num_pages (int or None): Number of pages to retrieve (default: 100).
        - retrieve_all (bool): Whether to fetch all pages in the database.
        - kwargs: Additional filters for querying Notion.

        Returns:
        - list: List of pages retrieved from Notion.
        """
        results = []
        payload = {"page_size": min(num_pages or 100, 100), **kwargs}

        while True:
            response = self.api.query_database(self.database_id, payload)
            results.extend(response.get("results", []))

            # Stop if there are no more pages
            if not response.get("has_more"):
                break

            # Use the next_cursor for pagination
            payload["start_cursor"] = response.get("next_cursor")

            # Stop if num_pages limit is reached and retrieve_all is False
            if not retrieve_all and num_pages and len(results) >= num_pages:
                break

        return results[:num_pages] if num_pages and not retrieve_all else results

    def get_title_property_name(self):
        """
        Determines the name of the title property in the database schema.
        Caches the result after first retrieval.

        Returns:
        - str: Title property name.
        """
        if self.title_property_name is None:
            schema = self.api.get_database(self.database_id)
            for prop_name, prop_value in schema.get("properties", {}).items():
                if prop_value.get("type") == "title":
                    self.title_property_name = prop_name
                    break

        return self.title_property_name

    def add_page(self, properties):
        """Add a new page to the Notion database."""
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": properties,
        }
        return self.api.create_page(payload)

    def update_page(self, page_id, properties):
        """Update a page in the Notion database."""
        payload = {"properties": properties}
        return self.api.update_page(page_id, payload)


if __name__ == "__main__":
    from oauthmanager import OnePasswordAuthManager
    import json

    # Retrieve Notion API key using OnePasswordAuthManager
    auth_manager = OnePasswordAuthManager(vault_name="API Keys")
    notion_creds = auth_manager.get_credentials("Quantum", "credential")
    NOTION_API_KEY = notion_creds.get("credential")

    # Notion Database ID
    DATABASE_ID = "16aa1865b187810cbb34e07ffd6b40b8"

    if NOTION_API_KEY:
        # Initialize NotionManager
        manager = NotionManager(api_key=NOTION_API_KEY, database_id=DATABASE_ID)

        # Fetch limited pages
        print("\n--- Fetching Limited Pages (5) ---")
        limited_pages = manager.get_pages(num_pages=5, retrieve_all=False)

        if limited_pages:
            print(f"Fetched {len(limited_pages)} pages.")

            # Test fetching a single page by ID
            first_page_id = limited_pages[0]["id"]
            print(f"\n--- Fetching Details for Page ID: {first_page_id} ---")
            page_details = manager.get_page(first_page_id)

            if page_details:
                print(json.dumps(page_details, indent=2))  # Pretty print the page details
            else:
                print("Failed to retrieve page details.")
        else:
            print("No pages retrieved.")

    else:
        print("Failed to retrieve Notion API key from OnePassword.")
