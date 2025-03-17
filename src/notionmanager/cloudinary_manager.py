import os
import re
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api

from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any

# -------------------------------------------------------------------
# Import helper functions from utils module.
# -------------------------------------------------------------------

from notionmanager.utils import (
    expand_or_preserve_env_vars,
    compute_file_hash,
    generate_tags,
    create_new_url
)

# -------------------------------------------------------------------
# NotionManager
# -------------------------------------------------------------------

from notionmanager.notion import NotionManager
