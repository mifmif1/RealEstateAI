"""
Database setup script for PostGIS
Run this script to initialize the database schema
"""
import logging
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

from database.config import get_db_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_migration(migration_file: Path, conn: psycopg2.extensions.connection):
    """Run a single migration file"""
    logger.info(f"Running migration: {migration_file.name}")
    
    with open(migration_file, 'r', encoding='utf-8') as f:
        migration_sql = f.read()

    # Work around any accidental tool-artifact text appended to the SQL file
    # (e.g. lines starting with 'C```assistant to=functions.ApplyPatch').
    # If such a marker is present, ignore everything after it.
    sentinel = "C```assistant to=functions.ApplyPatch"
    idx = migration_sql.find(sentinel)
    if idx != -1:
        migration_sql = migration_sql[:idx]
    
    with conn.cursor() as cursor:
        try:
            # Execute the whole migration file at once so that
            # functions, triggers, etc. with internal semicolons
            # or dollar-quoted strings are handled correctly by Postgres.
            cursor.execute(migration_sql)
            logger.debug(f"Executed migration file {migration_file.name}")
        except Exception as e:
            logger.error(f"Error executing migration {migration_file.name}: {e}")
            raise
    
    conn.commit()
    logger.info(f"Migration {migration_file.name} completed successfully")


def setup_database():
    """Set up the database schema"""
    config = get_db_config()
    
    try:
        logger.info(f"Connecting to database: {config.database}")
        conn = psycopg2.connect(config.psycopg2_connection_string)
        
        # Get migration files
        migrations_dir = Path(__file__).parent / "migrations"
        migration_files = sorted(migrations_dir.glob("*.sql"))
        
        if not migration_files:
            logger.warning("No migration files found")
            return
        
        logger.info(f"Found {len(migration_files)} migration file(s)")
        
        # Run migrations
        for migration_file in migration_files:
            run_migration(migration_file, conn)
        
        logger.info("Database setup completed successfully!")
        
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Please ensure PostgreSQL is running and credentials are correct")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    setup_database()

