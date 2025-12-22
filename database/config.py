"""Database configuration."""
import os
from pathlib import Path

# Database file path
DATABASE_DIR = Path(__file__).parent.parent / "database"
DATABASE_DIR.mkdir(exist_ok=True)

DATABASE_FILE = DATABASE_DIR / "realestateai.db"
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"

# Database settings
ECHO_SQL = os.getenv("DB_ECHO_SQL", "False").lower() == "true"
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))

