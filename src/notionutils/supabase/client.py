import os
from supabase import create_client, Client
from dotenv import load_dotenv

class SupabaseClient:
    def __init__(self):
        """
        Initialize the Supabase client using credentials from a .env file.
        """
        load_dotenv()
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_api_key = os.getenv("SUPABASE_API_KEY")

        if not supabase_url or not supabase_api_key:
            raise ValueError("Supabase credentials are missing. Check your .env file.")

        self.client: Client = create_client(supabase_url, supabase_api_key)

    def keep_alive(self):
        """
        Perform a lightweight query to keep the Supabase account active.
        """
        response = self.client.rpc("noop").execute()  # Replace "noop" with a lightweight stored procedure
        if not response.data:
            raise Exception(f"Keep-alive query failed: {response}")
        return response.data


if __name__ == "__main__":
    try:
        # Initialize the Supabase client
        client = SupabaseClient()
        print("Successfully connected to Supabase.")

        # Test the keep_alive method
        print("Calling keep_alive...")
        result = client.keep_alive()
        print("Keep-alive successful:", result)
    except Exception as e:
        print(f"Error: {e}")
