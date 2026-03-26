"""
FarndaCred configuration module.
Move major app and database settings here; override via environment variables in production.
"""

import os
from urllib.parse import quote_plus

def _env_with_legacy(new_key: str, legacy_key: str, default: str = "") -> str:
    return os.environ.get(new_key) or os.environ.get(legacy_key, default)


# 1. Database Credentials
# Prefer FARNDACRED_* keys, with LMS_* fallback for backward compatibility.
DB_USER = _env_with_legacy("FARNDACRED_DB_USER", "LMS_DB_USER", "postgres")
DB_PASSWORD = _env_with_legacy("FARNDACRED_DB_PASSWORD", "LMS_DB_PASSWORD", "")  # no real password in code
DB_HOST = _env_with_legacy("FARNDACRED_DB_HOST", "LMS_DB_HOST", "localhost")
DB_PORT = _env_with_legacy("FARNDACRED_DB_PORT", "LMS_DB_PORT", "5432")
DB_NAME = _env_with_legacy("FARNDACRED_DB_NAME", "LMS_DB_NAME", "lms_db")

def get_database_url() -> str:
    """Build PostgreSQL connection URL for lms_db."""
    # This checks if an environment variable exists first (useful for deployment)
    if os.environ.get("FARNDACRED_DATABASE_URL"):
        return os.environ.get("FARNDACRED_DATABASE_URL")
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
DEFAULT_INTEREST_METHOD = _env_with_legacy("FARNDACRED_INTEREST_METHOD", "LMS_INTEREST_METHOD", "Reducing balance")
DEFAULT_INTEREST_TYPE = _env_with_legacy("FARNDACRED_INTEREST_TYPE", "LMS_INTEREST_TYPE", "Compound")
DEFAULT_RATE_BASIS = _env_with_legacy("FARNDACRED_RATE_BASIS", "LMS_RATE_BASIS", "Per annum")

# Path to schema SQL (for programmatic init)
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schema")