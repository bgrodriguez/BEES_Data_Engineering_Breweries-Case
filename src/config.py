from pathlib import Path

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

BRONZE_PATH = Path("data/bronze")
SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")

LOG_PATH = Path("logs/pipeline.log")