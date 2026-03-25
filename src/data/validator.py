from dotenv import load_dotenv
import os
import logging

load_dotenv()

api_key = os.getenv("FMP_API_KEY")

if not api_key:
    raise ValueError("FMP_API_KEY not found. Did you create a .env file?")