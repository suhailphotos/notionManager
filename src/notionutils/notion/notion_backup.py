import requests

class NotionManager:
    def __init__(self, NOTION_API_KEY: str, DATABASE_ID: str = None, **kwargs):
        if not DATABASE_ID:
            raise ValueError("DATABASE_ID is required to initialize NotionManager")

        self.url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
        self.headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self.DATABASE_ID = DATABASE_ID

        # If kwargs are provided, assume they are filters or other query parameters
        self.pages = self._get_pages(**kwargs)

        # Determine the title property name once
        self.title_property_name = self._get_title_property_name()

    def _get_pages(self, num_pages=None, **kwargs):
        """
        Fetch pages from the Notion database, with optional filters or parameters.
    
        Parameters:
        - num_pages (int or None): The number of pages to fetch.
          - If None, all pages will be fetched using pagination.
          - If specified, it should be less than or equal to 100.
        - **kwargs: Additional parameters to apply to the query payload, such as filters.
    
        Returns:
        - list: A list of pages from the Notion database.
        """
        get_all = num_pages is None
        page_size = 100 if get_all else num_pages
    
        # Construct the payload with page size
        payload = {"page_size": page_size}
        
        # Add filters or other parameters from kwargs if they are provided
        if kwargs:
            payload.update(kwargs)
    
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
            
            # Include filters in subsequent requests if they are provided
            if kwargs:
                payload.update(kwargs)
    
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
    """
    Example usage of the NotionManager class.

    This section demonstrates how to initialize and use the NotionManager class
    to update the cover image of pages in a Notion database. By default, filtering
    is turned off. Uncomment the section labeled "With Filter" to enable filtering
    and only fetch pages where the "Fresh macOS Install" field is non-empty.

    Requirements:
    - Ensure you have valid Notion API credentials managed by OnePassword.
    - The "Fresh macOS Install" property should be a number type in your Notion database.

    Usage:
    """

    import re
    from oauthmanager import AuthManager, OnePasswordAuthManager
    from notion import NotionManager

    # Prompt the user for the Notion database URL and image URL
    database_url = input("Please enter the Notion database URL: ")
    image_url = input("Please enter the cover image URL: ")

    # Use regex to extract the DATABASE_ID from the URL
    match = re.search(r"\/([a-f0-9]{32})\?", database_url)
    if not match:
        print("Invalid Notion database URL provided. Please try again.")
        exit()
    DATABASE_ID = match.group(1)

    # Initialize the AuthManager and retrieve credentials for Notion
    auth_manager = OnePasswordAuthManager()
    notion_creds = auth_manager.get_credentials("Notion", "credential")
    NOTION_API_KEY = notion_creds['credential']

    # -----------------------
    # Default Version: Without Filter
    # -----------------------
    print("\n--- Running without filters ---")
    try:
        install_db = NotionManager(NOTION_API_KEY, DATABASE_ID)
        install_db.update_cover_image(cover_url=image_url)
    except Exception as e:
        print(f"Error: {e}")

    # -----------------------
    # Optional: With Filter
    # -----------------------
    # Uncomment the following lines to enable filtering
    """
    print("\n--- Running with filter ---")
    # Define the filter to only get entries where "Fresh macOS Install" is non-empty
    filter_condition = {
        "property": "Fresh macOS Install",
        "number": {
            "is_not_empty": True
        }
    }

    try:
        install_db_filtered = NotionManager(NOTION_API_KEY, DATABASE_ID, filter=filter_condition)
        install_db_filtered.update_cover_image(cover_url=image_url)
    except Exception as e:
        print(f"Error: {e}")
    """
