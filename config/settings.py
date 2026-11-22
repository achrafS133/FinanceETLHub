import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env file
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # API Keys
    EXCHANGE_RATE_API_KEY: str = Field(default="demo_key")
    BASE_CURRENCY: str = Field(default="GBP")
    TARGET_CURRENCIES: list = Field(default=["USD", "EUR", "MAD"])
    
    # Local DB
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="postgres")
    POSTGRES_DB: str = Field(default="finance_db")
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    
    # Microsoft Azure (Blob + Azure SQL)
    USE_AZURE: bool = Field(default=False)
    AZURE_STORAGE_CONNECTION_STRING: str = Field(default="")
    AZURE_STORAGE_CONTAINER: str = Field(default="finance-data")
    AZURE_SQL_CONNECTION_STRING: str = Field(default="")

    # Snowflake
    SNOWFLAKE_USER: str = Field(default="")
    SNOWFLAKE_PASSWORD: str = Field(default="")
    SNOWFLAKE_ACCOUNT: str = Field(default="")
    SNOWFLAKE_WAREHOUSE: str = Field(default="COMPUTE_WH")
    SNOWFLAKE_DATABASE: str = Field(default="FINANCE_DW")
    SNOWFLAKE_SCHEMA: str = Field(default="PUBLIC")
    USE_SNOWFLAKE: bool = Field(default=False)
    
    # App Settings
    LOG_LEVEL: str = Field(default="INFO")
    RAW_DATA_PATH: Path = Field(default=BASE_DIR / "data" / "raw")
    PROCESSED_DATA_PATH: Path = Field(default=BASE_DIR / "data" / "processed")
    DATASET_URL: str = Field(default="https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
