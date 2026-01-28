"""
SQL Server Database Connection

Provides a SQLAlchemy engine for connecting to Azure SQL Server.
Credentials are loaded from environment variables (.env file).

Connection features:
- Read-only access (ApplicationIntent=READONLY)
- Azure Active Directory password authentication
- Encrypted connection with server certificate validation disabled
- Uses ODBC Driver 18 for SQL Server

Usage:
    from db_connection import get_db_connection
    
    engine = get_db_connection()
    with engine.connect() as conn:
        result = conn.execute("SELECT * FROM my_table")
"""

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Load environment variables from the .env file
load_dotenv()

# Get credentials from environment variables
db_server = os.getenv("DB_SERVER")
db_database = os.getenv("DB_DATABASE")
db_uid = os.getenv("DB_UID")
db_password = os.getenv("DB_PASSWORD")

# Build SQLAlchemy connection string for Azure SQL Server
# - driver: ODBC Driver 18 for SQL Server (required for Azure)
# - Encrypt=yes: Force encrypted connection
# - TrustServerCertificate=no: Validate server certificate (Azure requirement)
# - ApplicationIntent=READONLY: Read-only connection (prevents accidental writes)
# - Authentication=ActiveDirectoryPassword: Use Azure AD authentication
connection_string = f"mssql+pyodbc://{db_uid}:{db_password}@{db_server}/{db_database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&ApplicationIntent=READONLY&Authentication=ActiveDirectoryPassword"

# Function to return the database connection
def get_db_connection():
    engine = create_engine(connection_string)
    return engine