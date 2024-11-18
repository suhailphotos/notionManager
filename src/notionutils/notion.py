import requests

class NotionManager:
    def __init__(self, NOTION_API_KEY: str, DATABASE_ID: str = None):
        if not DATABASE_ID:
            raise ValueError("DATABASE_ID is required to initialize NotionManager")

        self.url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
        self.headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self.DATABASE_ID = DATABASE_ID
        self.pages = self._get_pages()

        # Determine the title property name once
        self.title_property_name = self._get_title_property_name()

    def _get_pages(self, num_pages=None):
        get_all = num_pages is None
        page_size = 100 if get_all else num_pages

        payload = {"page_size": page_size}
        try:
            response = requests.post(self.url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch pages: {e}")
            return []

        results = data.get("results", [])
        while data.get("has_more") and get_all:
            payload = {"page_size": page_size, "start_cursor": data["next_cursor"]}
            try:
                response = requests.post(self.url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                results.extend(data.get("results", []))
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch more pages: {e}")
                break

        return results

    def _get_title_property_name(self):
        """Identify the property name that has the type 'title'."""
        if not self.pages:
            return None
        properties = self.pages[0].get("properties", {})
        for prop_name, prop_value in properties.items():
            if prop_value.get("type") == "title":
                return prop_name
        return None

    def update_cover_image(self, cover_url):
        if not self.title_property_name:
            print("Title property not found.")
            return

        # Iterate over all pages and update the cover image
        for page in self.pages:
            page_id = page["id"]
            properties = page.get("properties", {})
            title_property = properties.get(self.title_property_name, {})
            title_list = title_property.get("title", [])
            title = title_list[0].get("text", {}).get("content", "Untitled") if title_list else "Untitled"

            update_url = f"https://api.notion.com/v1/pages/{page_id}"
            update_payload = {
                "cover": {
                    "type": "external",
                    "external": {
                        "url": cover_url
                    }
                }
            }
            try:
                res = requests.patch(update_url, headers=self.headers, json=update_payload)
                if res.status_code == 200:
                    print(f"Updated page '{title}' successfully.")
                else:
                    print(f"Failed to update page '{title}': {res.status_code} - {res.text}")
            except requests.exceptions.RequestException as e:
                print(f"Error updating page '{title}': {e}")


if __name__ == "__main__":
    from oauthmanager import AuthManager, OnePasswordAuthManager

    # Initialize the AuthManager
    auth_manager = OnePasswordAuthManager()

    # Retrieve credential for Notion
    notion_creds = auth_manager.get_credentials("Notion", "credential")

    # Set the default API key
    DEFAULT_NOTION_API_KEY = notion_creds['credential']

    # Use NOTION_API_KEY if it's set; otherwise, use DEFAULT_NOTION_API_KEY
    NOTION_API_KEY = DEFAULT_NOTION_API_KEY
    DATABASE_ID = "13ca1865b187803390f4d1b6b0bd92e8"

    # Cover image URL (broken down for better readability)
    new_cover_url = "https://images.unsplash.com/photo-1690321607902-2799a1e8eaaa?q=80&w=3540&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
    install_db = NotionManager(NOTION_API_KEY, DATABASE_ID)
    install_db.update_cover_image(cover_url=new_cover_url)
