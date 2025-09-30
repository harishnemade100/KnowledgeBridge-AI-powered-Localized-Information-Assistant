# recommender/recommender.py
import sqlite3
from collections import Counter

DB_PATH = "crawler/storage.db"

class Recommender:
    def __init__(self):
        self.user_history = []

    def log_query(self, query):
        self.user_history.append(query)

    def recommend(self):
        if not self.user_history:
            return []
        keywords = Counter(self.user_history).most_common(3)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        recommendations = []
        for kw, _ in keywords:
            cur.execute("SELECT url, title, category FROM pages WHERE content LIKE ? LIMIT 3", (f"%{kw}%",))
            recommendations.extend(cur.fetchall())
        conn.close()
        return recommendations
