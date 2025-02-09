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

# Path to the CSV file generated from your script
csv_file_path = "/Users/suhail/Desktop/python_files1.csv"

def read_csv(csv_path):
    file_data = {}
    with open(csv_path, "r") as file:
        # Skip the header
        next(file)
        for line in file:
            filename, full_path = line.strip().split(",", 1)
            file_data[filename] = full_path
    return file_data

# Function to read the content of a Python file
def read_file_content(file_path):
    with open(file_path, "r") as file:
        return file.read()

# Function to insert code content into the Notion page
def insert_code_to_notion():
    file_data = read_csv(csv_file_path)

    # Query all pages in the Notion database
    results = notion.databases.query(database_id=database_id)["results"]

    for page in results:
        # Extract the page ID and properties
        page_id = page["id"]
        properties = page["properties"]
        
        # Get the filename from the "Filename" property
        filename_property = properties.get("Filename")
        if filename_property and filename_property["type"] == "title":
            filename = filename_property["title"][0]["text"]["content"]

            # Check if the filename exists in the CSV data
            if filename in file_data:
                file_path = file_data[filename]
                # Read the file content
                code_content = read_file_content(file_path)

                # Insert code content as a code block in the Notion page
                notion.blocks.children.append(
                    page_id,
                    children=[
                        {
                            "object": "block",
                            "type": "code",
                            "code": {
                                "rich_text": [
                                    {
                                        "type": "text",
                                        "text": {"content": code_content}
                                    }
                                ],
                                "language": "python"
                            }
                        }
                    ]
                )
                print(f"Inserted code content into page: {filename}")

# Run the function
insert_code_to_notion()
