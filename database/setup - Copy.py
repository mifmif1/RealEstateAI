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
    
    with conn.cursor() as cursor:
        # Split by semicolons but keep them in the statements
        statements = [s.strip() + ';' for s in migration_sql.split(';') if s.strip()]
        
        for statement in statements:
            try:
                cursor.execute(statement)
                logger.debug(f"Executed: {statement[:50]}...")
            except Exception as e:
                logger.error(f"Error executing statement: {e}")
                logger.error(f"Statement: {statement}")
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

