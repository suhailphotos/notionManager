import os
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api
from pathlib import Path
from dotenv import load_dotenv


class CloudinaryManager:
    def __init__(self, cloud_name=None, api_key=None, api_secret=None, **config):
        """
        Initializes CloudinaryManager with credentials.
        
        - Uses passed-in credentials if provided.
        - Otherwise, loads from .env in src/notionmanager (for dev) or ~/.notionmanager/.env (for production).
        - Raises an error if credentials are missing.
        """
        # Load environment variables (dev first, then prod)
        env_path = Path(__file__).parent / ".env"  # Dev
        prod_env_path = Path.home() / ".notionmanager" / ".env"  # Prod

        if env_path.exists():
            load_dotenv(env_path)
        elif prod_env_path.exists():
            load_dotenv(prod_env_path)

        # Get credentials, prioritizing parameters
        self.cloud_name = cloud_name or os.getenv("CLOUDINARY_CLOUD_NAME")
        self.api_key = api_key or os.getenv("CLOUDINARY_API_KEY")
        self.api_secret = api_secret or os.getenv("CLOUDINARY_API_SECRET")

        # Validate credentials
        if not all([self.cloud_name, self.api_key, self.api_secret]):
            raise ValueError("Cloudinary credentials missing. Provide manually or in .env.")

        # Configure Cloudinary
        cloudinary.config(
            cloud_name=self.cloud_name,
            api_key=self.api_key,
            api_secret=self.api_secret,
            **config
        )

    def upload_assets(self, folder_path, root_category, add_tags=True, output_json="upload_results.json"):
        """
        Uploads images from a folder to Cloudinary under /banner/ or /icon/.

        Args:
            - folder_path (str): Path to the folder (supports env vars like $DROPBOX).
            - root_category (str): Either 'banner' or 'icon' to determine Cloudinary folder.
            - add_tags (bool): Whether to tag images based on folder hierarchy.
            - output_json (str): Filename to save JSON results.

        Returns:
            - JSON file with original file names, Cloudinary URLs, and tags.
        """
        folder_path = os.path.expandvars(folder_path)  # Expand environment variables
        folder_path = Path(folder_path).resolve()      # Convert to absolute path

        if not folder_path.exists():
            raise FileNotFoundError(f"Folder {folder_path} does not exist.")

        # Determine allowed formats based on category
        supported_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".avif"}
        if root_category == "icon":
            supported_extensions.add(".svg")  # Allow SVG for icons

        uploaded_files = []

        for image_path in folder_path.rglob("*"):
            if image_path.suffix.lower() in supported_extensions:
                # Make the relative path based on the folder itself, NOT the parent
                # to avoid duplicating "banner" or "icon".
                relative_path = image_path.relative_to(folder_path)

                tags = self._generate_tags(relative_path, root_category) if add_tags else []

                # Upload to Cloudinary, preserving original filename
                response = cloudinary.uploader.upload(
                    str(image_path),
                    folder=f"{root_category}/",
                    tags=tags,
                    use_filename=True,      # Use original file name
                    unique_filename=False   # Don't append random chars
                )

                # Store results
                upload_entry = {
                    "original_filename": image_path.name,
                    "cloudinary_url": response["secure_url"],
                    "tags": tags
                }
                uploaded_files.append(upload_entry)
                print(f"Uploaded: {image_path.name} → {response['secure_url']}")

        # Save results to JSON file
        with open(output_json, "w") as json_file:
            json.dump(uploaded_files, json_file, indent=4)

        return uploaded_files

    def _generate_tags(self, relative_path, root_category):
        """
        Generates tags based on the folder hierarchy.

        Example:
        - File: /path/to/banner/programming/matplotlib.jpg
        - Tags: ['banner', 'programming']
        """
        # root_category is always included as the first tag
        # The rest are derived from subfolders (excluding the final file name).
        tag_list = [root_category] + list(relative_path.parts[:-1])
        # Convert tags to lowercase and replace spaces with underscores
        return [tag.lower().replace(" ", "_") for tag in tag_list]


# Example Usage
if __name__ == "__main__":
    cloudinary_manager = CloudinaryManager()  # Load from .env

    # Upload Banners
    uploaded_banners = cloudinary_manager.upload_assets(
        "$DROPBOX/matrix/packages/notionUtils/assets/media/banner",
        root_category="banner"
    )

