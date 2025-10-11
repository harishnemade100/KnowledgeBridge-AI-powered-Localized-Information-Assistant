# database.py
import sqlite3

DB_PATH = "storage.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        title TEXT,
        summary TEXT,
        content TEXT,
        category TEXT,
        language TEXT,
        content_hash TEXT UNIQUE,
        last_crawled TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # FTS5 virtual table for fast text search
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
        url, title, summary, content, category, language, content='pages', content_rowid='id'
    )""")
    # triggers to keep FTS in sync
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
        INSERT INTO pages_fts(rowid, url, title, summary, content, category, language)
        VALUES (new.id, new.url, new.title, new.summary, new.content, new.category, new.language);
    END;
    """)
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
        DELETE FROM pages_fts WHERE rowid=old.id;
    END;
    """)
    conn.commit()
    conn.close()

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
