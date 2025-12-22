"""Database setup and initialization."""
from database.connection import init_db, engine
from database.models import Base


def setup_database():
    """Initialize the database by creating all tables."""
    print("Initializing database...")
    init_db()
    print("Database initialized successfully!")


if __name__ == "__main__":
    setup_database()

