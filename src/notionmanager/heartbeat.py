from prefect import flow, task, get_run_logger
from notionmanager.supabase import SupabaseClient

@task
def keep_supabase_active(env_path: str = "/opt/prefect/.env"):
    """
    Task to perform a lightweight query to keep the Supabase account active.
    """
    logger = get_run_logger()
    client = SupabaseClient(env_path=env_path)
    try:
        response = client.keep_alive()
        logger.info("Keep-alive task executed successfully: %s", response)
        return response
    except Exception as e:
        logger.error("Keep-alive task failed: %s", e)
        raise

@flow
def supabase_keep_alive_flow(env_path: str = "/opt/prefect/.env"):
    """
    Flow to execute the keep-alive task.
    """
    logger = get_run_logger()
    logger.info("Starting Supabase Keep-Alive Flow...")
    keep_supabase_active(env_path=env_path)
    logger.info("Supabase Keep-Alive Flow completed successfully.")

if __name__ == "__main__":
    # Test the flow locally
    supabase_keep_alive_flow()
