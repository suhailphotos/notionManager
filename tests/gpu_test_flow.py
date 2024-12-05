from prefect import flow, task
import torch

@task
def check_gpu():
    if torch.cuda.is_available():
        gpu_name = torch.cuda.get_device_name(0)
        print(f"GPU Available: {gpu_name}")
        return f"GPU Available: {gpu_name}"
    else:
        print("No GPU Found.")
        return "No GPU Found."

@flow
def gpu_test_flow():
    result = check_gpu()
    print(result)

if __name__ == "__main__":
    gpu_test_flow()
