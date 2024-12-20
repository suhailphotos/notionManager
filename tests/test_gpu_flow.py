from prefect import flow, task, get_run_logger
from prefect_github import GitHubCredentials

@task
def test_gpu_task():
    logger = get_run_logger()
    import torch
    if torch.cuda.is_available():
        device = torch.device("cuda")
        message = f"GPU is available: {torch.cuda.get_device_name(0)}"
    else:
        message = "No GPU is available."
    logger.info(message)  # Log the GPU availability message
    return message

@flow
def test_gpu_flow():
    logger = get_run_logger()
    gpu_message = test_gpu_task()
    logger.info(f"Flow result: {gpu_message}")  # Log the flow result

if __name__ == "__main__":
    github_credentials_block = GitHubCredentials.load("github-credentials")
    logger = get_run_logger()
    logger.info(f"Loaded GitHub credentials: {github_credentials_block}")
    test_gpu_flow()
