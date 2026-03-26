-- Run this script connected to the default 'postgres' database (or as superuser).
-- Creates the FarndaCred application database for PostgreSQL 18.

-- Omit LC_COLLATE/LC_CTYPE on Windows if not available; they are optional.
CREATE DATABASE lms_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0;

COMMENT ON DATABASE lms_db IS 'FarndaCred application database';
