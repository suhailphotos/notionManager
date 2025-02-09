from notionmanager.notion.api import NotionAPI

class NotionManager:
    def __init__(self, api_key, database_id, cache_pages=False):
        """
        Initialize the NotionManager with API key and database ID.

        Parameters:
        - api_key (str): Notion API key.
        - database_id (str): ID of the database to manage.
        - cache_pages (bool): Whether to cache all pages on initialization.
        """
        self.api = NotionAPI(api_key)
        self.database_id = database_id
        self.cache_pages = cache_pages
        self.pages = self._get_pages(retrieve_all=True) if cache_pages else None
        self.title_property_name = self._get_title_property_name()

    def _get_pages(self, num_pages=None, retrieve_all=True, **kwargs):
        """
        Fetch pages from the database, handling pagination and optional limits.

        Parameters:
        - num_pages (int or None): Number of pages to retrieve. Defaults to 100 if not specified.
        - retrieve_all (bool): Whether to fetch all pages in the database. Overrides num_pages if True.
        - **kwargs: Additional filters or query parameters.

        Returns:
        - list: List of pages retrieved from the Notion database.
        """
        # If retrieve_all is True, ignore num_pages
        if retrieve_all:
            num_pages = None

        results = []
        payload = {"page_size": min(num_pages or 100, 100), **kwargs}

        while True:
            response = self.api.query_database(self.database_id, payload)
            results.extend(response.get("results", []))

            # Stop if there are no more pages
            if not response.get("has_more"):
                break

            # Use the next_cursor for subsequent requests
            payload["start_cursor"] = response.get("next_cursor")

            # Stop if num_pages limit is reached and retrieve_all is False
            if not retrieve_all and num_pages and len(results) >= num_pages:
                break

        return results[:num_pages] if num_pages and not retrieve_all else results

    def _get_title_property_name(self):
        """Determine the name of the title property."""
        schema = self.api.get_database(self.database_id)
        for prop_name, prop_value in schema.get("properties", {}).items():
            if prop_value.get("type") == "title":
                return prop_name
        return None

    def add_page(self, properties):
        """Add a new page to the database."""
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": properties,
        }
        return self.api.create_page(payload)

    def update_page(self, page_id, properties):
        """Update a page in the database."""
        payload = {"properties": properties}
        return self.api.update_page(page_id, payload)

    def get_cached_pages(self):
        """Return cached pages or fetch them if not already cached."""
        if not self.cache_pages or self.pages is None:
            self.pages = self._get_pages(retrieve_all=True)
        return self.pages

if __name__ == "__main__":
    from oauthmanager import OnePasswordAuthManager

    # Retrieve Notion API key using OnePasswordAuthManager
    auth_manager = OnePasswordAuthManager(vault_name="API Keys")
    notion_creds = auth_manager.get_credentials("Quantum", "credential")  # Corrected call
    NOTION_API_KEY = notion_creds.get("credential")  # Safe retrieval

    # Notion Database ID
    DATABASE_ID = "16aa1865b187810cbb34e07ffd6b40b8"

    if NOTION_API_KEY:
        # Initialize NotionManager
        manager = NotionManager(api_key=NOTION_API_KEY, database_id=DATABASE_ID)

        # Default behavior: Fetch all pages
        print("\n--- Default: Fetching All Pages ---")
        all_pages = manager._get_pages()
        print(f"Fetched {len(all_pages)} pages.")

        # Fetch limited pages
        print("\n--- Fetching Limited Pages (5) ---")
        limited_pages = manager._get_pages(num_pages=5, retrieve_all=False)
        print(f"Fetched {len(limited_pages)} pages.")

    else:
        print("Failed to retrieve Notion API key from OnePassword.")
