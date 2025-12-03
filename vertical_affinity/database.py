"""
Database connection management for MySQL and Trino.
"""
import sys
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from trino import dbapi
from trino.auth import BasicAuthentication
from vertical_affinity.config import (
    DATABASE_URL,
    TRINO_HOST,
    TRINO_PORT,
    TRINO_USER,
    TRINO_CATALOG,
    TRINO_SCHEMA
)


def create_mysql_engine():
    """
    Create and return a MySQL database engine.
    
    Returns:
        sqlalchemy.engine.Engine: MySQL database engine
        
    Raises:
        SystemExit: If connection fails
    """
    try:
        engine = create_engine(DATABASE_URL)
        print("✅ 数据库连接引擎创建成功。")
        return engine
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        sys.exit(1)


def create_trino_connection():
    """
    Create and return a Trino database connection.
    
    Returns:
        trino.dbapi.Connection: Trino database connection
    """
    conn = dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )
    return conn

