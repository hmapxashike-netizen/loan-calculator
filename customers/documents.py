import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional

def _env_with_legacy(new_key: str, legacy_key: str, default: str) -> str:
    return os.environ.get(new_key) or os.environ.get(legacy_key, default)

def get_db_connection():
    return psycopg2.connect(
        host=_env_with_legacy("FARNDACRED_DB_HOST", "LMS_DB_HOST", "localhost"),
        port=_env_with_legacy("FARNDACRED_DB_PORT", "LMS_DB_PORT", "5432"),
        dbname=_env_with_legacy("FARNDACRED_DB_NAME", "LMS_DB_NAME", "farndacred_db"),
        user=_env_with_legacy("FARNDACRED_DB_USER", "LMS_DB_USER", "postgres"),
        password=_env_with_legacy("FARNDACRED_DB_PASSWORD", "LMS_DB_PASSWORD", "postgres"),
    )

# --- Document Classes ---

def list_document_classes(active_only: bool = True) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if active_only:
                cur.execute("SELECT * FROM document_classes WHERE is_active = TRUE ORDER BY name")
            else:
                cur.execute("SELECT * FROM document_classes ORDER BY name")
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def create_document_class(name: str, description: str = "") -> int:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO document_classes (name, description)
                VALUES (%s, %s)
                RETURNING id
            """, (name, description))
            conn.commit()
            return cur.fetchone()[0]
    finally:
        conn.close()

def update_document_class(class_id: int, name: str, description: str, is_active: bool) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE document_classes
                SET name = %s, description = %s, is_active = %s
                WHERE id = %s
            """, (name, description, is_active, class_id))
            conn.commit()
    finally:
        conn.close()

# --- Document Categories ---

def list_document_categories(active_only: bool = True) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT dc.*, cl.name as class_name 
                FROM document_categories dc
                LEFT JOIN document_classes cl ON dc.class_id = cl.id
            """
            if active_only:
                query += " WHERE dc.is_active = TRUE"
            query += " ORDER BY cl.name, dc.name"
            cur.execute(query)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def create_document_category(name: str, description: str = "", class_id: Optional[int] = None) -> int:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO document_categories (name, description, class_id)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, description, class_id))
            conn.commit()
            return cur.fetchone()[0]
    finally:
        conn.close()

def update_document_category(category_id: int, name: str, description: str, is_active: bool, class_id: Optional[int] = None) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE document_categories
                SET name = %s, description = %s, is_active = %s, class_id = %s
                WHERE id = %s
            """, (name, description, is_active, class_id, category_id))
            conn.commit()
    finally:
        conn.close()


# --- Documents ---

def upload_document(
    entity_type: str,
    entity_id: int,
    category_id: int,
    file_name: str,
    file_type: str,
    file_size: int,
    file_content: bytes,
    uploaded_by: str = "",
    notes: str = ""
) -> int:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO documents (
                    entity_type, entity_id, category_id, file_name, file_type, 
                    file_size, file_content, uploaded_by, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                entity_type, entity_id, category_id, file_name, file_type, 
                file_size, file_content, uploaded_by, notes
            ))
            conn.commit()
            return cur.fetchone()[0]
    finally:
        conn.close()

def list_documents(entity_type: str = None, entity_id: int = None) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT d.id, d.entity_type, d.entity_id, d.category_id, 
                       c.name as category_name, d.file_name, d.file_type, 
                       d.file_size, d.uploaded_by, d.uploaded_at, d.notes
                FROM documents d
                LEFT JOIN document_categories c ON d.category_id = c.id
                WHERE 1=1
            """
            params = []
            if entity_type:
                query += " AND d.entity_type = %s"
                params.append(entity_type)
            if entity_id is not None:
                query += " AND d.entity_id = %s"
                params.append(entity_id)
            
            query += " ORDER BY d.uploaded_at DESC"
            
            cur.execute(query, tuple(params))
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def get_document(document_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT d.*, c.name as category_name
                FROM documents d
                LEFT JOIN document_categories c ON d.category_id = c.id
                WHERE d.id = %s
            """, (document_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()

def delete_document(document_id: int) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM documents WHERE id = %s", (document_id,))
            conn.commit()
    finally:
        conn.close()
