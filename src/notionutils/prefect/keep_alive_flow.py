from prefect import flow, task, get_run_logger
from src.notionutils.supabase.client import SupabaseClient  

@task
def keep_supabase_active():
    """
    Task to perform a lightweight query to keep the Supabase account active.
    """
    logger = get_run_logger()
    client = SupabaseClient()
    try:
        response = client.keep_alive()
        logger.info("Keep-alive task executed successfully: %s", response)
        return response
    except Exception as e:
        logger.error("Keep-alive task failed: %s", e)
        raise

@flow
def supabase_keep_alive_flow():
    """
    Flow to execute the keep-alive task.
    """
    logger = get_run_logger()
    logger.info("Starting Supabase Keep-Alive Flow...")
    keep_supabase_active()
    logger.info("Supabase Keep-Alive Flow completed successfully.")

if __name__ == "__main__":
    # Test the flow locally
    supabase_keep_alive_flow()
