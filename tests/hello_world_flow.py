from prefect import flow, task

# Define a task (a unit of work)
@task
def say_hello(name: str):
    print(f"Hello, {name}!")
    return f"Hello, {name}!"

# Define a flow (a collection of tasks)
@flow
def hello_world_flow():
    result = say_hello("World")
    print(result)

# Entry point for running locally
if __name__ == "__main__":
    hello_world_flow()
