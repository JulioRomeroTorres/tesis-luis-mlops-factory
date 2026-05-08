import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from typing import Optional

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):
    app_name: str = "Prediction Pm25"
    api_version: str = "1.0.0"
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"

    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://20.14.210.156:3001"
    ]
    
    firestore_db_name: Optional[str] = os.getenv("FIRESTORE_DB_NAME")

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
