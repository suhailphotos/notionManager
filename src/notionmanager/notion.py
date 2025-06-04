import json
from notionmanager.api import NotionAPI

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

    def add_page(self, notion_payload: dict):
        """
        Create a new Notion page by passing the final JSON payload to the API.

        Expected keys might include:
          "parent":       { "database_id": <your DB> }
          "properties":   { ... }
          "cover":        { ... }
          "icon":         { ... }
          etc.

        If the user does *not* provide a "parent", we default
        to using self.database_id as the parent database.
        """
        if "parent" not in notion_payload:
            notion_payload["parent"] = {"database_id": self.database_id}

        return self.api.create_page(notion_payload)

    def update_page(self, page_id, properties):
        """Update a page in the Notion database."""
        payload = {"properties": properties}
        return self.api.update_page(page_id, payload)

    def update_cover(self, page_id, cover_payload):
        """Update the cover of a Notion page."""
        payload = {"cover": cover_payload}
        return self.api.update_page(page_id, payload)

    def update_icon(self, page_id, icon_payload):
        """Update the icon of a Notion page."""
        payload = {"icon": icon_payload}
        return self.api.update_page(page_id, payload)

    def delete_page(self, page_id):
        # Mark the page as archived.
        payload = {"archived": True}
        return self.api.update_page(page_id, payload)

    def transform_page(self, page, properties_mapping):
        """
        Transform a single Notion page into a simpler dictionary, based on a mapping.
    
        The properties_mapping is a dict where each key is the Notion property name,
        and its value is another dict with:
          - "target": the key to use in the output
          - "type": (optional) the Notion type, e.g. "title", "rich_text", "url", 
                    "relation", "select", "multi_select"
          - "return": desired output style ("str", "object", or "list")
    
        For example:
          properties_mapping = {
              "id": {"target": "id", "return": "str"},
              "icon": {"target": "icon", "return": "object"},
              "cover": {"target": "cover", "return": "object"},
              "Name": {"target": "name", "type": "title", "return": "str"},
              "Tool": {"target": "tool", "type": "relation", "return": "list"},
              "Type": {"target": "type", "type": "select", "return": "list"},
              "Course Description": {"target": "description", "type": "rich_text", "return": "str"},
              "Course Link": {"target": "link", "type": "url", "return": "str"},
              "Path": {"target": "path", "type": "rich_text", "return": "str"},
              "Template": {"target": "template", "type": "rich_text", "return": "str"},
              "Tags": {"target": "tags", "type": "multi_select", "return": "list"}
          }
        """
        transformed = {}
        for notion_prop, config in properties_mapping.items():
            target_key = config.get("target")
            prop_type = config.get("type")  # e.g. "rich_text", "title", "url", etc.
            ret_type = config.get("return") # "str", "object", "list"
    
            # Get the property value from the page.
            if "properties" in page and notion_prop in page["properties"]:
                raw = page["properties"][notion_prop]
            elif notion_prop in page:
                raw = page[notion_prop]
            else:
                raw = None
    
            if raw is None:
                transformed[target_key] = None
            else:
                if ret_type == "object":
                    # Return the entire raw property.
                    transformed[target_key] = raw
                elif ret_type == "str":
                    if prop_type == "status":
                        # Extract the name of the status option
                        st = raw.get("status", {})
                        transformed[target_key] = st.get("name")
                    elif prop_type in ("rich_text", "title"):
                        # Notion may split text into multiple pieces; join them.
                        items = raw.get(prop_type, [])
                        joined = "".join(item.get("plain_text", "") for item in items)
                        transformed[target_key] = joined
                    elif prop_type == "url":
                        transformed[target_key] = raw.get("url")
                    elif prop_type == "select":
                        select_obj = raw.get("select")
                        transformed[target_key] = select_obj.get("name") if select_obj else None
                    else:
                        transformed[target_key] = str(raw)
                elif ret_type == "boolean":
                    # Handle checkbox properties as booleans
                    if prop_type == "checkbox":
                        transformed[target_key] = raw.get("checkbox", False)
                    else:
                        transformed[target_key] = bool(raw)
                elif ret_type == "list":
                    if prop_type == "relation":
                        # Return a list of relation IDs.
                        relations = raw.get("relation", [])
                        transformed[target_key] = [rel.get("id") for rel in relations]
                    elif prop_type == "multi_select":
                        # Return a list of names.
                        multi = raw.get("multi_select", [])
                        transformed[target_key] = [item.get("name") for item in multi]
                    elif prop_type == "select":
                        # Wrap the select name in a list.
                        select_obj = raw.get("select")
                        transformed[target_key] = [select_obj.get("name")] if select_obj else []
                    else:
                        # Fallback: if the raw value is already a list, use it;
                        # otherwise wrap it in a list.
                        transformed[target_key] = raw if isinstance(raw, list) else [raw]
                else:
                    transformed[target_key] = raw
        return transformed

    def transform_pages(self, pages, properties_mapping):
        """
        Transform one or multiple Notion pages using transform_page().

        If 'pages' is a list, returns a list of transformed pages.
        If 'pages' is a single page (dict), returns the transformed page.
        """
        if isinstance(pages, list):
            return [self.transform_page(page, properties_mapping) for page in pages]
        else:
            return self.transform_page(pages, properties_mapping)


    def build_hierarchy(self, pages, hierarchy, properties_mapping, parent_field="Parent item"):
        """
        Build a nested hierarchy from a flat list of Notion pages based on a flexible hierarchy configuration.

        Parameters:
          - pages: a flat list of Notion page dictionaries.
          - hierarchy: a dict defining the hierarchy. For example:
                {
                  "root": "courses",       # key for top-level items (level 0)
                  "level_1": "chapters",     # key for children of root (level 1)
                  "level_2": "lessons"       # key for children of level_1 (level 2)
                }
          - properties_mapping: mapping to transform Notion properties (see transform_page).
          - parent_field: the name of the Notion property holding the parent relation (default "Parent item").

        Returns:
          A nested dictionary whose top-level key is hierarchy["root"] and whose items have
          nested children under keys defined for each level.
        """
        # Build a mapping from page id to transformed node.
        node_map = {}
        for page in pages:
            node = self.transform_page(page, properties_mapping)
            # Determine the parent(s) using the specified parent_field.
            parent_prop = page.get("properties", {}).get(parent_field, {})
            relations = parent_prop.get("relation", []) if parent_prop else []
            parent_ids = [rel.get("id") for rel in relations] if relations else []
            node["_parent_ids"] = parent_ids
            node["_level"] = None  # Level to be computed
            node_map[page["id"]] = node

        # Recursive helper to compute a node's level based on its parent chain.
        def compute_level(node):
            if node["_level"] is not None:
                return node["_level"]
            if not node["_parent_ids"]:
                node["_level"] = 0
                return 0
            # Assume one parent (if multiple, use the first)
            parent_id = node["_parent_ids"][0]
            parent_node = node_map.get(parent_id)
            if not parent_node:
                node["_level"] = 0
                return 0
            level = compute_level(parent_node) + 1
            node["_level"] = level
            return level

        # Compute levels for all nodes.
        for node in node_map.values():
            compute_level(node)

        # Build the tree by attaching non-root nodes to their parent.
        roots = []
        for node in node_map.values():
            if node["_level"] == 0:
                roots.append(node)
            else:
                parent_id = node["_parent_ids"][0] if node["_parent_ids"] else None
                parent_node = node_map.get(parent_id)
                if parent_node:
                    child_level = node["_level"]
                    # Look up the key for this child level.
                    hierarchy_key = hierarchy.get(f"level_{child_level}", "children")
                    if hierarchy_key not in parent_node:
                        parent_node[hierarchy_key] = []
                    parent_node[hierarchy_key].append(node)

        # Cleanup internal keys.
        def cleanup(node):
            node.pop("_parent_ids", None)
            node.pop("_level", None)
            for key in hierarchy.values():
                if key in node:
                    for child in node[key]:
                        cleanup(child)

        for node in roots:
            cleanup(node)

        root_key = hierarchy.get("root", "root")
        return {root_key: roots}

    def build_notion_payload(self, flat_object, mapping, parent_database_id=None):
        """
        Build a Notion page payload from a single processed (flat) object.

        Parameters:
          - flat_object (dict): The processed object (e.g. a course, chapter, or lesson).
          - mapping (dict): The mapping configuration for back-transformation. For example:
                {
                  "icon": {"target": "icon", "return": "object"},
                  "cover": {"target": "cover", "return": "object"},
                  "name": {"target": "Name", "type": "title", "return": "str"},
                  "tool": {"target": "Tool", "type": "relation", "return": "list", "property_id": "pvso"},
                  "type": {"target": "Type", "type": "select", "return": "list", "property_id": "DCuB"},
                  "description": {"target": "Course Description", "type": "rich_text", "return": "str", "property_id": "XQwN"},
                  "link": {"target": "Course Link", "type": "url", "return": "str", "property_id": "O%3AZR"},
                  "path": {"target": "Path", "type": "rich_text", "return": "str", "property_id": "%3Eua%3C", "code": True},
                  "template": {"target": "Template", "type": "rich_text", "return": "str", "property_id": "NBdS", "code": True},
                  "tags": {"target": "Tags", "type": "multi_select", "return": "list", "property_id": "tWcF"}
                }
          - parent_database_id (str or None): The database ID for the page's parent.
            If None, self.database_id is used.

        Returns:
          dict: A dictionary representing the Notion page payload.
        """
        payload = {"object": "page"}
        parent_db = parent_database_id if parent_database_id else self.database_id
        payload["parent"] = {"type": "database_id", "database_id": parent_db}

        # Process cover and icon separately.
        if "cover" in flat_object and flat_object["cover"]:
            cover_val = flat_object["cover"]
            if isinstance(cover_val, dict) and cover_val.get("type") == "external":
                cover_url = cover_val.get("external", {}).get("url")
            else:
                cover_url = cover_val
            if cover_url:
                payload["cover"] = {"type": "external", "external": {"url": cover_url}}
        if "icon" in flat_object and flat_object["icon"]:
            icon_val = flat_object["icon"]
            if isinstance(icon_val, dict) and icon_val.get("type") == "external":
                icon_url = icon_val.get("external", {}).get("url")
            else:
                icon_url = icon_val
            if icon_url:
                payload["icon"] = {"type": "external", "external": {"url": icon_url}}

        # Build properties.
        props = {}
        for flat_key, conf in mapping.items():
            # Skip icon and cover so they don't get repeated in the properties.
            if flat_key in ("icon", "cover"):
                continue

            notion_prop = conf.get("target")
            prop_type = conf.get("type")
            ret_type = conf.get("return")
            property_id = conf.get("property_id")
            code_flag = conf.get("code", False)
            value = flat_object.get(flat_key)
            if value is None:
                continue  # Skip missing properties.
            # For "object" types, copy the value as is.
            if ret_type == "object":
                prop_payload = value
            elif prop_type in ("rich_text", "title"):
                text_item = {
                    "type": "text",
                    "text": {"content": value, "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": code_flag,
                        "color": "default"
                    },
                    "plain_text": value,
                    "href": None
                }
                if prop_type == "title":
                    prop_payload = {"type": "title", "title": [text_item]}
                else:
                    prop_payload = {"type": "rich_text", "rich_text": [text_item]}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "url":
                prop_payload = {"type": "url", "url": value}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "relation":
                relations = []
                if isinstance(value, list):
                    for rel in value:
                        if rel:
                            relations.append({"id": rel})
                else:
                    relations.append({"id": value})
                prop_payload = {"type": "relation", "relation": relations}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "select":
                select_name = value[0] if isinstance(value, list) and value else value
                prop_payload = {"type": "select", "select": {"name": select_name} if select_name else None}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "multi_select":
                multi = []
                if isinstance(value, list):
                    for item in value:
                        multi.append({"name": item})
                else:
                    multi.append({"name": value})
                prop_payload = {"type": "multi_select", "multi_select": multi}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "checkbox":
                # Build payload for checkbox properties
                prop_payload = {"type": "checkbox", "checkbox": bool(value)}
                if property_id:
                    prop_payload["id"] = property_id
            elif prop_type == "status":
                # Build payload for status properties
                prop_payload = {
                    "type": "status",
                    "status": {"name": value}
                }
                if property_id:
                    prop_payload["id"] = property_id
            else:
                # Fallback to rich_text.
                text_item = {
                    "type": "text",
                    "text": {"content": str(value), "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": code_flag,
                        "color": "default"
                    },
                    "plain_text": str(value),
                    "href": None
                }
                prop_payload = {"type": "rich_text", "rich_text": [text_item]}
                if property_id:
                    prop_payload["id"] = property_id
            props[notion_prop] = prop_payload

        payload["properties"] = props
        return payload


if __name__ == "__main__":
    import os
    import json
    from oauthmanager import OnePasswordAuthManager

    def load_notion_credentials():
        # Retrieve Notion API key using OnePasswordAuthManager
        auth_manager = OnePasswordAuthManager(vault_name="API Keys")
        notion_creds = auth_manager.get_credentials("Quantum", "credential")
        return notion_creds.get("credential")

    def load_course_database_info():

        # Notion Database ID
        DATABASE_ID = "195a1865-b187-8103-9b6a-cc752ca45874"

        # Example properties mapping for forward transformation.
        properties_mapping = {
            "id": {"target": "id", "return": "str"},
            "icon": {"target": "icon", "return": "object"},
            "cover": {"target": "cover", "return": "object"},
            "Name": {"target": "name", "type": "title", "return": "str"},
            "Tool": {"target": "tool", "type": "relation", "return": "list"},
            "Type": {"target": "type", "type": "select", "return": "list"},
            "Course Description": {"target": "description", "type": "rich_text", "return": "str"},
            "Course Link": {"target": "link", "type": "url", "return": "str"},
            "Instructor": {"target": "instructor", "type": "select", "return": "list"},
            "Institute": {"target":"institute", "type":"select", "return":"list"},
            "Path": {"target": "path", "type": "rich_text", "return": "str"},
            "Template": {"target": "template", "type": "rich_text", "return": "str", "default": "default"},
            "Tags": {"target": "tags", "type": "multi_select", "return": "list"},
            "Status": {"target": "status",     "type": "status",    "return": "str"},
            "Video": {"target": "video", "type": "checkbox", "return": "boolean"},
            "Video Path": {"target": "video_path", "type": "rich_text", "return": "str"}
        }

        # Example back-transformation mapping.
        back_mapping = {
            "icon": {"target": "icon", "return": "object"},
            "cover": {"target": "cover", "return": "object"},
            "name": {"target": "Name", "type": "title", "return": "str"},
            "tool": {"target": "Tool", "type": "relation", "return": "list", "default": ["149a1865-b187-80f9-b21f-c9c96430bf62"]},
            "type": {"target": "Type", "type": "select", "return": "list"},
            "description": {"target": "Course Description", "type": "rich_text", "return": "str"},
            "link": {"target": "Course Link", "type": "url", "return": "str"},
            "instructor": {"target": "Instructor", "type": "select", "return": "list"},
            "institute": {"target":"Institute", "type":"select", "return":"list"},
            "path": {"target": "Path", "type": "rich_text", "return": "str", "code": True},
            "template": {"target": "Template", "type": "rich_text", "return": "str", "code": True},
            "tags": {"target": "Tags", "type": "multi_select", "return": "list", "default": ["Python"]},
            "status": {"target": "Status", "type": "status",    "return": "str", "default": "Not started"},
            "video": {"target": "Video", "type": "checkbox", "return": "boolean"},
            "video_path": {"target": "Video Path", "type": "rich_text", "return": "str", "code": True}
        }
        # Example hierarchy configuration for a three-level structure.
        hierarchy_config = {
            "root": "courses",       # Top-level key for courses (level 0)
            "level_1": "chapters",     # Children of courses are chapters (level 1)
            "level_2": "lessons"       # Children of chapters are lessons (level 2)
        }

        return DATABASE_ID, properties_mapping, back_mapping, hierarchy_config

    def test_page_transform_payload():
        NOTION_API_KEY = load_notion_credentials()
        DATABASE_ID, properties_mapping, back_mapping, hierarchy_config = load_course_database_info()

        if NOTION_API_KEY:
            # Initialize NotionManager
            manager = NotionManager(api_key=NOTION_API_KEY, database_id=DATABASE_ID)

        filter = {
            "filter": {
                "or": [
                    {"property": "Name", "title": {"equals": "Machine Learning in VFX"}},
                    {"property": "Name", "title": {"equals": "Week 2"}},
                    {"property": "Name", "title": {"equals": "Data Intro"}},
                    {"property": "Name", "title": {"equals": "Databases"}}
                ]
            }
        }
        # Retrieve pages based on the filter.
        course_pages = manager.get_pages(**filter)
        if course_pages:
            # For demonstration, transform the first page and then rebuild the Notion payload.
            transformed_page = manager.transform_page(course_pages[0], properties_mapping)
            notion_payload = manager.build_notion_payload(transformed_page, back_mapping)
            print("Transformed Page:")
            print(json.dumps(transformed_page, indent=2))
            print("\nNotion Payload:")
            print(json.dumps(notion_payload, indent=2))
        else:
            print("No pages retrieved.")

    def test_add_lesson():
        """
        Load one lesson from the ml_3d_wk3_vfx.json payload and insert it into Notion
        under the specified parent page ID, printing both the payload and the response.
        """
        NOTION_API_KEY = load_notion_credentials()
        DATABASE_ID, properties_mapping, back_mapping, _ = load_course_database_info()

        # Initialize manager
        manager = NotionManager(api_key=NOTION_API_KEY, database_id=DATABASE_ID)

        # Load sample payload file
        payload_path = os.path.expanduser("~/.incept/payload/ml_3d_wk3_vfx.json")
        with open(payload_path, "r") as f:
            data = json.load(f)

        # Grab the first lesson in week 3
        lesson = data["courses"][0]["chapters"][0]["lessons"][0]
        # Ensure a status is present
        lesson.setdefault("status", "Not started")

        # Build the Notion payload
        notion_payload = manager.build_notion_payload(lesson, back_mapping)

        # Attach as a sub-item under the given parent page
        notion_payload.setdefault("properties", {})
        notion_payload["properties"]["Parent item"] = {
            "type": "relation",
            "relation": [{"id": "1f8a1865-b187-8105-a17b-fa3a79898855"}]
        }

        # Print and send
        print("=== Add Lesson Payload ===")
        print(json.dumps(notion_payload, indent=2))
        response = manager.add_page(notion_payload)
        print("=== Add Lesson Response ===")
        print(json.dumps(response, indent=2))

    # test_page_transform_payload()
    test_add_lesson()
