import requests

class NotionAPI:
    BASE_URL = "https://api.notion.com/v1/"

    def __init__(self, api_key, version="2022-06-28"):
        self.api_key = api_key  # Store API key for authentication
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Notion-Version": version,
        }

    def query_database(self, database_id, payload=None):
        """Query a Notion database."""
        url = f"{self.BASE_URL}databases/{database_id}/query"
        response = requests.post(url, headers=self.headers, json=payload or {})
        response.raise_for_status()
        return response.json()

    def create_page(self, payload):
        """Create a new page in a Notion database."""
        url = f"{self.BASE_URL}pages"
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def update_page(self, page_id, payload):
        """Update an existing Notion page."""
        url = f"{self.BASE_URL}pages/{page_id}"
        response = requests.patch(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def get_database(self, database_id):
        """Retrieve database schema and properties."""
        url = f"{self.BASE_URL}databases/{database_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_page(self, page_id):
        """Fetch a single Notion page by its ID."""
        url = f"{self.BASE_URL}pages/{page_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()  # Return the response

