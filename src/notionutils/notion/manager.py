from notionutils.notion.api import NotionAPI

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
        self.pages = self._get_pages() if cache_pages else None
        self.title_property_name = self._get_title_property_name()

    def _get_pages(self, num_pages=None, **kwargs):
        """Fetch pages from the database, optionally with filters."""
        payload = {"page_size": num_pages or 100, **kwargs}
        return self.api.query_database(self.database_id, payload).get("results", [])

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
            self.pages = self._get_pages()
        return self.pages
