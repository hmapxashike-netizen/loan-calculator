import psycopg2
import bcrypt
from config import get_database_url


def bootstrap_admin():
    email = "hmapxashike@gmail.com"          # CHANGE ME to your chosen email
    password = "NewStrongPassword123!"  # CHANGE ME to your chosen password
    full_name = "Herbert Farai Mapxashike"

    hashed_pw = bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt(rounds=12),
    ).decode("utf-8")

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(get_database_url())
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (email, password_hash, full_name, role, is_active)
            VALUES (%s, %s, %s, 'ADMIN', TRUE)
            ON CONFLICT (email) DO UPDATE
                SET password_hash = EXCLUDED.password_hash,
                    is_active = TRUE,
                    failed_login_attempts = 0,
                    locked_until = NULL;
            """,
            (email, hashed_pw, full_name),
        )
        conn.commit()
        print(f"Admin user {email} is ready.")
        print(f"Use password: {password}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    bootstrap_admin()