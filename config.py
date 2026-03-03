"""
LMS configuration module.
Move major app and database settings here; override via environment variables in production.
"""

import os
from urllib.parse import quote_plus

# 1. Database Credentials
# Using the credentials from your recent successful psql sessions.
DB_USER = "postgres"
DB_PASSWORD = "M1k@y1@2017"  
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "lms_db"

def get_database_url() -> str:
    """Build PostgreSQL connection URL for lms_db."""
    # This checks if an environment variable exists first (useful for deployment)
    if os.environ.get("LMS_DATABASE_URL"):
        return os.environ.get("LMS_DATABASE_URL")
    
    # URL-encode the password to handle special characters like '@' 
    # This prevents the "could not translate host name" error.
    safe_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
    
    # Build the string using the variables defined above
    auth = f"{DB_USER}:{safe_password}" if safe_password else DB_USER
    return f"postgresql://{auth}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -----------------------------------------------------------------------------
# App / defaults (for future use)
# -----------------------------------------------------------------------------
DEFAULT_INTEREST_METHOD = os.environ.get("LMS_INTEREST_METHOD", "Reducing balance")
DEFAULT_INTEREST_TYPE = os.environ.get("LMS_INTEREST_TYPE", "Compound")
DEFAULT_RATE_BASIS = os.environ.get("LMS_RATE_BASIS", "Per annum")

# Path to schema SQL (for programmatic init)
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schema")