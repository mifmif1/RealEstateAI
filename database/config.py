"""
Database configuration for PostGIS connection
"""
import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', '5432'))
    database: str = os.getenv('DB_NAME', 'realestate_ai')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', 'postgres')
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def psycopg2_connection_string(self) -> str:
        """Get connection string for psycopg2"""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


def get_db_config() -> DatabaseConfig:
    """Get database configuration from environment variables or defaults"""
    return DatabaseConfig()

