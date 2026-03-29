import psycopg2
from config import get_database_url

def run():
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute('''
            CREATE TABLE IF NOT EXISTS customer_approval_drafts (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_id INTEGER NOT NULL,
                action_type VARCHAR(50) NOT NULL,
                old_details JSONB,
                new_details JSONB NOT NULL,
                requested_by VARCHAR(100),
                approved_by VARCHAR(100),
                supporting_document TEXT,
                status VARCHAR(50) DEFAULT 'PENDING',
                submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                approved_at TIMESTAMP WITH TIME ZONE,
                dismissed_note TEXT,
                dismissed_at TIMESTAMP WITH TIME ZONE,
                rework_note TEXT,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            ''')
            cur.execute('''
            CREATE TABLE IF NOT EXISTS customer_name_history (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                agent_id INTEGER REFERENCES agents(id) ON DELETE CASCADE,
                old_name VARCHAR(255) NOT NULL,
                new_name VARCHAR(255) NOT NULL,
                requested_by_id VARCHAR(100),
                approved_by_id VARCHAR(100),
                supporting_document_url TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            ''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_customer_name_history_customer_id ON customer_name_history(customer_id);')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_customer_name_history_agent_id ON customer_name_history(agent_id);')

        conn.commit()
        print('Migration 55 applied.')
    except Exception as e:
        conn.rollback()
        print('Error:', e)
    finally:
        conn.close()

if __name__ == '__main__':
    run()