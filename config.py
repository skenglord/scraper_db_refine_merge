import os
from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    MONGODB_LOCAL_URI: str = os.getenv("MONGODB_LOCAL_URI", "mongodb://localhost:27017/")
    SCRAPER_DEFAULT_OUTPUT_DIR: str = "output"
    SCRAPER_DEFAULT_MIN_DELAY: float = 2.5
    SCRAPER_DEFAULT_MAX_DELAY: float = 6.0
    SCRAPER_DEFAULT_HEADLESS: bool = True
    # Add other environment variables as needed, with type hints and default values.
    # Example: API_KEY: str

    class Config:
        # If you want to load variables from a .env file:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore" # Ignore extra fields from .env file

# Instantiate the settings
settings = Settings()

# You can then import 'settings' from this module in other parts of your application
# from config import settings
