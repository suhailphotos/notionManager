from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
supabase_url = os.getenv("SUPABASE_URL")
supabase_api_key = os.getenv("SUPABASE_API_KEY")

# Test if the variables are loaded
if supabase_url and supabase_api_key:
    print("dotenv is working!")
    print(f"SUPABASE_URL: {supabase_url}")
    print(f"SUPABASE_API_KEY: {supabase_api_key[:4]}... (truncated)")
else:
    print("dotenv failed to load the environment variables.")
