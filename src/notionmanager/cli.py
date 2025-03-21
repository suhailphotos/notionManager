import os
import json
import click
import shutil
from dotenv import load_dotenv
from pathlib import Path

# Define configuration directory and .env file location
CONFIG_DIR = Path.home() / ".notionmanager"
ENV_FILE = CONFIG_DIR / ".env"

@click.group()
def main():
    """
    Notion Manager CLI: Command line interface for managing Notion assets.
    """
    pass

@main.command("init")
def cli_init():
    """
    Initialize Notion Manager configuration by copying default configuration files,
    templates, and payload samples into the user's configuration directory.

    This command copies the following files from the source .config directory:
      - env.example (renamed to .env)
      - sync_config.json
    """
    click.echo("Initializing Notion Manager configuration")

    # Determine the source configuration directory relative to this file.
    config_source = Path(__file__).parent / ".config"

    # Ensure the user config directory exists.
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    files_to_copy = [
        {
            "src_file_name": "env.example",
            "dest_file_name": ".env",
            "src_file_path": ".config",
            "overwrite_if_exists": False
        },
        {
            "src_file_name": "sync_config.json",
            "dest_file_name": "sync_config.json",
            "src_file_path": ".config",
            "overwrite_if_exists": False
        }
    ]

    for file in files_to_copy:
        src_subdir = Path(__file__).parent / file["src_file_path"]
        src_file = src_subdir / file["src_file_name"]
        dest_file = CONFIG_DIR / file["dest_file_name"]

        if file["overwrite_if_exists"]:
            shutil.copy2(src_file, dest_file)
            click.echo(f"Copied {src_file} to {dest_file} (overwritten).")
        else:
            if not dest_file.exists():
                shutil.copy2(src_file, dest_file)
                click.echo(f"Copied {src_file} to {dest_file}.")
            else:
                click.echo(f"{dest_file} already exists; skipping.")

@main.command("sync")
@click.option("--job", help="Name of the sync job to run.", default=None)
@click.option("--all", "run_all", is_flag=True, help="Run all sync jobs.")
def cli_sync(job, run_all):
    """
    Run sync jobs based on your configuration.
    """
    click.echo("Running sync jobs...")

    # Load sync configuration.
    from notionmanager.config import load_sync_config
    config = load_sync_config()
    sync_jobs = config.get("sync_jobs", [])
    if not sync_jobs:
        click.echo("No sync jobs found in configuration.")
        return

    # Create an instance of CloudinaryManager.
    from notionmanager.cloudinary_manager import CloudinaryManager
    cloud_manager = CloudinaryManager()

    # Determine which jobs to run.
    if run_all:
        jobs_to_run = sync_jobs
    elif job:
        jobs_to_run = [j for j in sync_jobs if j.get("name") == job]
        if not jobs_to_run:
            click.echo(f"No sync job found with name: {job}")
            return
    else:
        click.echo("Please provide either --job or --all option.")
        return

    # Process each sync job.
    for job_cfg in jobs_to_run:
        job_name = job_cfg.get("name")
        folder_path = job_cfg.get("path")
        method = job_cfg.get("method", {})
        method_type = method.get("type")
        click.echo(f"Running sync job '{job_name}'...")

        if method_type == "notiondb":
            from notionmanager.backends import NotionSyncBackend, NotionDBConfig
            notiondb_cfg = method.get("notiondb", {})
            notion_forward = method.get("forward_mapping", {})
            notion_reverse = method.get("reverse_mapping", {})
            db_id = notiondb_cfg.get("id")
            default_icon = notiondb_cfg.get("default_icon", {})

            notion_api_key = os.getenv("NOTION_API_KEY", "YOUR_NOTION_API_KEY")
            notion_db_config = NotionDBConfig(
                database_id=db_id,
                forward_mapping=notion_forward,
                back_mapping=notion_reverse,
                default_icon=default_icon
            )
            sync_backend = NotionSyncBackend(notion_api_key, notion_db_config)

        elif method_type == "jsonlog":
            from notionmanager.backends import LocalJsonSyncBackend
            jsonlog_cfg = method.get("jsonlog", {})
            log_file_name = jsonlog_cfg.get("file_name", "sync_log.json")
            in_folder = jsonlog_cfg.get("in_folder", True)
            log_path = jsonlog_cfg.get("log_path", "")
            if in_folder:
                from notionmanager.utils import expand_or_preserve_env_vars
                expanded_folder, _ = expand_or_preserve_env_vars(folder_path)
                dest_file = expanded_folder / log_file_name
            else:
                dest_file = Path(log_path) / log_file_name if log_path else Path(log_file_name)
            sync_backend = LocalJsonSyncBackend(str(dest_file))

        else:
            click.echo(f"Unknown method type: {method_type}")
            continue

        # Run the sync for this job.
        cloud_manager.update_assets(
            folder_path=folder_path,
            root_category=job_name,
            sync_backend=sync_backend
        )

    click.echo("Sync jobs complete.")

if __name__ == "__main__":
    main()
