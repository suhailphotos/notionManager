from prefect import flow, task, get_run_logger
import os

# Define a task to log file and directory information
@task
def log_file_info():
    logger = get_run_logger()
    
    # Get the current file's absolute and relative paths
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)

    logger.info(f"Current file: {current_file}")
    logger.info(f"Current directory: {current_dir}")
    
    # List files in the current directory
    files_in_dir = os.listdir(current_dir)
    logger.info(f"Files in the current directory: {files_in_dir}")

    # List files in the parent directory
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    parent_files = os.listdir(parent_dir)
    logger.info(f"Files in the parent directory: {parent_files}")

# Define the flow
@flow
def hello_world_debug_flow():
    log_file_info()

# Entry point for running locally
if __name__ == "__main__":
    hello_world_debug_flow()
