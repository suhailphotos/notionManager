from prefect import flow, task
from prefect_github import GitHubCredentials

@task
def test_gpu_task():
    import torch
    if torch.cuda.is_available():
        device = torch.device("cuda")
        message = f"GPU is available: {torch.cuda.get_device_name(0)}"
    else:
        message = "No GPU is available."
    print(message)
    return message

@flow
def test_gpu_flow():
    gpu_message = test_gpu_task()
    print(f"Flow result: {gpu_message}")

if __name__ == "__main__":
    github_credentials_block = GitHubCredentials.load("github-credentials")
    print(f"Loaded GitHub credentials: {github_credentials_block}")
    test_gpu_flow()
