# indexer/indexer.py
import os
import sqlite3
import subprocess


script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)


CRAWLER_PATH = os.path.join(project_root, "crawler_spider", "crawler.py")
DB_PATH = os.path.join(project_root, "crawler_spider", "storage.db")


def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        content TEXT,
        category TEXT,
        content_hash TEXT
    )
    """)
    conn.commit()
    conn.close()

def run_crawler():
    print("⚡ No data found, running crawler...")
    subprocess.run(["python", CRAWLER_PATH], check=True)



def is_db_empty():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pages")
    count = cur.fetchone()[0]
    conn.close()
    return count == 0
   

def search(query, limit=10):
    ensure_db()
    
    # If DB is empty → trigger crawler automatically
    if is_db_empty():
        run_crawler()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT url, title, content, category FROM pages WHERE content LIKE ? LIMIT ?", (f"%{query}%", limit))
    results = cur.fetchall()
    conn.close()
    return results
