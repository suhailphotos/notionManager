import re

def extract_database_id_from_url(database_url):
    """Extract the database ID from a Notion database URL."""
    match = re.search(r"([a-f0-9]{32})", database_url)
    if not match:
        raise ValueError("Invalid Notion database URL.")
    return match.group(1)
