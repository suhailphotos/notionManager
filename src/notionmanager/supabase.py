import os
from supabase import create_client, Client
from dotenv import load_dotenv

class SupabaseClient:
    def __init__(self, env_path: str = None):
        """
        Initialize the Supabase client using credentials from a .env file.

        Args:
            env_path (str): Optional path to the .env file. If not provided, it will use the default behavior of `load_dotenv`.
        """
        # Load environment variables
        if env_path:
            load_dotenv(dotenv_path=env_path)
        else:
            load_dotenv()  # Default behavior

        # Retrieve credentials
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_api_key = os.getenv("SUPABASE_API_KEY")

        if not self.supabase_url or not self.supabase_api_key:
            raise ValueError("Supabase credentials are missing. Check your .env file or environment variables.")

        # Initialize the Supabase client
        self.client: Client = create_client(self.supabase_url, self.supabase_api_key)

    def keep_alive(self):
        """
        Perform a lightweight query to keep the Supabase account active.

        Returns:
            Response data from the query.
        """
        response = self.client.rpc("noop").execute()  # Replace "noop" with a lightweight stored procedure
        if not response.data:
            raise Exception(f"Keep-alive query failed: {response}")
        return response.data


if __name__ == "__main__":
    try:
        # Example usage
        env_file_path = "/opt/prefect/.env"  # Pass this path when using in Prefect
        client = SupabaseClient(env_path=env_file_path)

