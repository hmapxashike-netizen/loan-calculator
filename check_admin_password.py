import psycopg2
import bcrypt
from config import get_database_url

email = "hmapxashike@gmail.com"   # change to your real admin email
password = "Password123!"               # change to the password you think it is

conn = psycopg2.connect(get_database_url())
cur = conn.cursor()
cur.execute("SELECT password_hash FROM users WHERE email = %s", (email,))
row = cur.fetchone()
print("Row:", row)
if row:
    print("Match:", bcrypt.checkpw(password.encode("utf-8"), row[0].encode("utf-8")))
cur.close()
conn.close()

