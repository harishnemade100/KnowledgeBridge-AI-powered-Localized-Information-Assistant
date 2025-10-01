import sqlite3
import re
from collections import Counter


class Indexer:
    def __init__(self, db_path="storage.db"):
        self.db_path = db_path


    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


    def ensure_tables(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY, url TEXT UNIQUE, title TEXT, content TEXT, category TEXT, content_hash TEXT, last_crawled TIMESTAMP
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS search_history (
        id INTEGER PRIMARY KEY, query TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS reading_history (
        id INTEGER PRIMARY KEY, url TEXT, category TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        conn.close()


    def has_pages(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) as c FROM pages")
        r = cur.fetchone()
        conn.close()
        return (r[0] or 0) > 0


    def log_search(self, q):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO search_history (query) VALUES (?)", (q,))
        conn.commit()
        conn.close()


    def get_popular_searches(self, limit=10):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT query, COUNT(*) as cnt FROM search_history GROUP BY query ORDER BY cnt DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return [dict(query=r[0], count=r[1]) for r in rows]


    def _tokenize(self, text):
        tokens = re.findall(r"\w+", text.lower())
        return tokens


    def _score(self, doc_tokens, q_tokens, title_tokens):
        # simple scoring: tf + title bonus
        c = Counter(doc_tokens)
        score = sum(c[t] for t in q_tokens)
        score = self._score(doc_tokens, q_tokens, title_tokens)
        title_bonus = sum(1 for t in q_tokens if t in title_tokens)
        return score + title_bonus * 2  # title matches weigh more
    
    def search(self, q, category=None, limit=20):
        q_tokens = self._tokenize(q)
        conn = self._connect()
        cur = conn.cursor()
        if category:
            cur.execute("SELECT * FROM pages WHERE category = ?", (category,))
            rows = cur.fetchall()
        else:
            cur.execute("SELECT * FROM pages")
            rows = cur.fetchall()

        candidates = []
        for r in rows:
            content = r["content"] or ""
            title = r["title"] or ""
            doc_tokens = self._tokenize(content)
            title_tokens = self._tokenize(title)
            score = self._score(doc_tokens, q_tokens, title_tokens)
            if score > 0:
                candidates.append((score, dict(id=r["id"], url=r["url"], title=title, category=r["category"])))
        conn.close()
        candidates.sort(key=lambda x: x[0], reverse=True)
        return [c[1] for c in candidates[:limit]]
    
