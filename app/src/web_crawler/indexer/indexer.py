import sqlite3
import re
from collections import Counter, defaultdict
from typing import List, Dict, Any
from app.src.web_crawler.crawler_spider.crawler import EnhancedCrawler

class Indexer:
    def __init__(self, db_path="storage.db"):
        self.db_path = db_path
        self.inverted_index = defaultdict(set)
        self.ensure_tables()
        self._build_inverted_index()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_tables(self):
        """Ensure all required tables exist"""
        conn = self._connect()
        cur = conn.cursor()
        
        cur.execute("""CREATE TABLE IF NOT EXISTS pages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE,
                        title TEXT,
                        content TEXT,
                        category TEXT,
                        content_hash TEXT,
                        language TEXT DEFAULT 'english',
                        last_crawled TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS search_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        query TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS pagerank (
                        page_id INTEGER PRIMARY KEY,
                        score REAL DEFAULT 1.0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        
        conn.commit()
        conn.close()
        print("✅ Database tables ensured")

    def _build_inverted_index(self):
        """Build inverted index from database"""
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT id, title, content FROM pages")
            
            for row in cur.fetchall():
                doc_id = row["id"]
                text = f"{row['title']} {row['content']}".lower()
                tokens = self._tokenize(text)
                for token in tokens:
                    self.inverted_index[token].add(doc_id)
            
            conn.close()
            print(f"✅ Inverted index built with {len(self.inverted_index)} terms")
        except Exception as e:
            print(f"Error building inverted index: {e}")

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words"""
        if not text:
            return []
        return re.findall(r"\b\w+\b", text.lower())

    def _score_document(self, doc_tokens: List[str], title_tokens: List[str], query_tokens: List[str]) -> float:
        """Calculate relevance score for document"""
        # Term frequency in document
        doc_counter = Counter(doc_tokens)
        title_counter = Counter(title_tokens)
        
        # Basic term frequency score with title boost
        content_score = sum(doc_counter.get(token, 0) for token in query_tokens)
        title_score = sum(title_counter.get(token, 0) * 3 for token in query_tokens)
        
        return content_score + title_score

    def search(self, query: str, category: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Search documents with given query"""
        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        # Store search history
        self._store_search_query(query)

        conn = self._connect()
        cur = conn.cursor()
        
        # Build query
        if category:
            cur.execute("SELECT * FROM pages WHERE category = ?", (category,))
        else:
            cur.execute("SELECT * FROM pages")
        
        rows = cur.fetchall()
        conn.close()

        # Score and rank documents
        candidates = []
        for row in rows:
            doc_tokens = self._tokenize(row["content"] or "")
            title_tokens = self._tokenize(row["title"] or "")
            
            score = self._score_document(doc_tokens, title_tokens, query_tokens)
            
            if score > 0:
                candidates.append({
                    "score": score,
                    "id": row["id"],
                    "url": row["url"],
                    "title": row["title"] or "No Title",
                    "category": row["category"],
                    "content": row["content"]
                })
        
        # Sort by score and return top results
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:limit]

    def search_with_inverted(self, query: str, category: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Search using inverted index"""
        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        # Store search history
        self._store_search_query(query)

        # Get document IDs from inverted index
        doc_ids = set()
        for token in query_tokens:
            if token in self.inverted_index:
                doc_ids.update(self.inverted_index[token])
        
        if not doc_ids:
            return self.search(query, category, limit)  # Fallback to regular search

        conn = self._connect()
        cur = conn.cursor()
        
        # Fetch documents
        placeholders = ",".join("?" for _ in doc_ids)
        query_sql = f"SELECT * FROM pages WHERE id IN ({placeholders})"
        params = list(doc_ids)
        
        if category:
            query_sql += " AND category = ?"
            params.append(category)
            
        cur.execute(query_sql, params)
        rows = cur.fetchall()
        conn.close()

        # Score documents
        candidates = []
        for row in rows:
            doc_tokens = self._tokenize(row["content"] or "")
            title_tokens = self._tokenize(row["title"] or "")
            
            score = self._score_document(doc_tokens, title_tokens, query_tokens)
            
            if score > 0:
                candidates.append({
                    "score": score,
                    "id": row["id"],
                    "url": row["url"],
                    "title": row["title"] or "No Title",
                    "category": row["category"],
                    "content": row["content"]
                })
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:limit]

    def map_query_to_category(self, query: str) -> str:
        """Map search query to relevant category"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["health", "medical", "hospital", "covid", "vaccine", "doctor","covid-19", "corona", "स्वास्थ्य", "चिकित्सा"]):
            return "health" 
        elif any(word in query_lower for word in ["agriculture", "farm", "crop", "krishi", "farmer", "soil", "कृषि", "फसल", "किसान"]):
            return "agriculture"
        elif any(word in query_lower for word in ["education", "school", "college", "exam", "student", "teacher", "शिक्षा", "विद्यालय", "छात्र"]):
            return "education"
        else:
            return None

    def _store_search_query(self, query: str):
        """Store search query in history"""
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("INSERT INTO search_history (query) VALUES (?)", (query,))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error storing search query: {e}")

    def get_popular_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get popular search queries"""
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT query, COUNT(*) as count 
            FROM search_history 
            GROUP BY query 
            ORDER BY count DESC 
            LIMIT ?
        """, (limit,))
        
        results = [dict(row) for row in cur.fetchall()]
        conn.close()
        return results

    def get_statistics(self) -> Dict[str, Any]:
        """Get search engine statistics"""
        conn = self._connect()
        cur = conn.cursor()
        
        stats = {}
        
        # Page counts by category
        cur.execute("SELECT category, COUNT(*) as count FROM pages GROUP BY category")
        stats["pages_by_category"] = {row["category"]: row["count"] for row in cur.fetchall()}
        
        # Total pages
        cur.execute("SELECT COUNT(*) as total FROM pages")
        stats["total_pages"] = cur.fetchone()["total"]
        
        # Total searches
        cur.execute("SELECT COUNT(*) as total FROM search_history")
        stats["total_searches"] = cur.fetchone()["total"]
        
        conn.close()
        return stats


class RuralSearchEngine:
    def __init__(self, db_path="storage.db"):
        self.db_path = db_path
        self.indexer = Indexer(db_path=db_path)
        self._init_user_tables()

    def _init_user_tables(self):
        """Initialize user-related tables"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS user_searches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        query TEXT,
                        category TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        
        conn.execute("""CREATE TABLE IF NOT EXISTS user_interests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        category TEXT,
                        interest_score REAL DEFAULT 1.0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        conn.commit()
        conn.close()

    def crawl_and_index(self, categories=None, keywords=None):
        """Run crawl and rebuild index"""
        print("🔄 Starting crawl and index...")
        crawler = EnhancedCrawler()
        crawler.run_crawl(categories=categories, keywords=keywords)
        self.indexer._build_inverted_index()
        print("✅ Crawl and index completed")

    def search(self, query: str, user_id: str = "default", category: str = None, limit: int = 10):
        """Enhanced search with personalization"""
        # Store user search
        self._store_user_search(user_id, query, category)
        
        # Update user interests
        self._update_user_interests(user_id, query, category)
        
        # Auto-detect category if not provided
        if not category:
            category = self.indexer.map_query_to_category(query)
            print(f"🔍 Auto-detected category: {category}")

        # Perform search
        results = self.indexer.search_with_inverted(query, category, limit)
        
        # Add personalized ranking
        personalized_results = self._personalize_results(results, user_id)
        
        return personalized_results

    def _store_user_search(self, user_id: str, query: str, category: str):
        """Store user search history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("INSERT INTO user_searches (user_id, query, category) VALUES (?, ?, ?)",
                       (user_id, query, category))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error storing user search: {e}")

    def _update_user_interests(self, user_id: str, query: str, category: str):
        """Update user interest scores based on searches"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            if category:
                cur.execute("""INSERT OR REPLACE INTO user_interests 
                              (user_id, category, interest_score) 
                              VALUES (?, ?, COALESCE((SELECT interest_score + 0.1 FROM user_interests 
                              WHERE user_id=? AND category=?), 1.1))""",
                          (user_id, category, user_id, category))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error updating user interests: {e}")

    def _personalize_results(self, results: List[Dict], user_id: str) -> List[Dict]:
        """Personalize search results based on user interests"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT category, interest_score FROM user_interests WHERE user_id=?", (user_id,))
            user_interests = {row[0]: row[1] for row in cur.fetchall()}
            conn.close()
            
            # Boost scores for preferred categories
            for result in results:
                category = result.get('category')
                if category in user_interests:
                    result['score'] *= user_interests[category]
                    
            # Re-sort by personalized score
            results.sort(key=lambda x: x['score'], reverse=True)
            return results
        except Exception as e:
            print(f"Error personalizing results: {e}")
            return results

    def get_personalized_feed(self, user_id: str, limit: int = 5):
        """Get personalized knowledge feed for user"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            # Get user's top interests
            cur.execute("""SELECT category FROM user_interests 
                          WHERE user_id=? ORDER BY interest_score DESC LIMIT 1""", (user_id,))
            top_interest = cur.fetchone()
            
            if not top_interest:
                return self._get_trending_content(limit)
            
            top_category = top_interest[0]
            
            # Get recent content from top interest category
            cur.execute("""SELECT url, title, content, category 
                          FROM pages WHERE category=? 
                          ORDER BY last_crawled DESC LIMIT ?""", 
                       (top_category, limit))
            
            feed_items = []
            for row in cur.fetchall():
                feed_items.append({
                    'url': row[0],
                    'title': row[1],
                    'content_preview': row[2][:200] + '...' if len(row[2]) > 200 else row[2],
                    'category': row[3]
                })
            
            conn.close()
            return feed_items
            
        except Exception as e:
            print(f"Error getting personalized feed: {e}")
            return self._get_trending_content(limit)

    def _get_trending_content(self, limit: int = 5):
        """Get trending content as fallback"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""SELECT url, title, content, category 
                          FROM pages ORDER BY last_crawled DESC LIMIT ?""", (limit,))
            
            trending = []
            for row in cur.fetchall():
                trending.append({
                    'url': row[0],
                    'title': row[1],
                    'content_preview': row[2][:200] + '...' if len(row[2]) > 200 else row[2],
                    'category': row[3]
                })
            conn.close()
            return trending
        except Exception as e:
            print(f"Error getting trending content: {e}")
            return []

    def get_search_suggestions(self, query: str):
        """Get search suggestions based on query"""
        popular_searches = self.indexer.get_popular_searches(limit=10)
        
        suggestions = []
        query_lower = query.lower()
        
        for search in popular_searches:
            if query_lower in search['query'].lower():
                suggestions.append(search['query'])
        
        # Add category-based suggestions
        category = self.indexer.map_query_to_category(query)
        if category:
            if category == 'health':
                suggestions.extend(['covid vaccine', 'hospital near me', 'health scheme', 'medical insurance'])
            elif category == 'agriculture':
                suggestions.extend(['crop prices', 'fertilizer scheme', 'soil health', 'weather forecast'])
            elif category == 'education':
                suggestions.extend(['school admission', 'exam results', 'scholarship', 'online courses'])
        
        return list(set(suggestions))[:5]  # Remove duplicates and limit

    def _get_user_search_history(self, user_id: str, limit: int = 20):
        """Get user search history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""SELECT query, category, timestamp 
                          FROM user_searches 
                          WHERE user_id=? 
                          ORDER BY timestamp DESC 
                          LIMIT ?""", (user_id, limit))
            
            history = []
            for row in cur.fetchall():
                history.append({
                    'query': row[0],
                    'category': row[1],
                    'timestamp': row[2]
                })
            conn.close()
            return history
        except Exception as e:
            print(f"Error getting user history: {e}")
            return []

    def _get_user_interests(self, user_id: str):
        """Get user interests"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""SELECT category, interest_score 
                          FROM user_interests 
                          WHERE user_id=? 
                          ORDER BY interest_score DESC""", (user_id,))
            
            interests = []
            for row in cur.fetchall():
                interests.append({
                    'category': row[0],
                    'score': row[1]
                })
            conn.close()
            return interests
        except Exception as e:
            print(f"Error getting user interests: {e}")
            return []

    def _clear_user_history(self, user_id: str):
        """Clear user search history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM user_searches WHERE user_id=?", (user_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error clearing user history: {e}")