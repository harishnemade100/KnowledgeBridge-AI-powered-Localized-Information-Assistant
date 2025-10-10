import time
import socket
import hashlib
import sqlite3
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from app.src.web_crawler.crawler_spider.seeds import PRIMARY_SEEDS
import random

DB_PATH = "storage.db"


class EnhancedCrawler:
    def __init__(self, db_path=DB_PATH, politeness=1.5, max_pages=200):
        self.db_path = db_path
        self.politeness = politeness
        self.max_pages = max_pages
        self.session = requests.Session()

        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET"])
        )

        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (X11; Linux x86_64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X)"
        ]

    def _get_headers(self, host=None):
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html"
        }
        if host:
            headers["Host"] = host
        return headers

    def _connect_db(self):
        conn = sqlite3.connect(self.db_path)
        return conn

    def _clean_text(self, html):
        soup = BeautifulSoup(html, "html.parser")

        # Remove unwanted tags
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        main = soup.find("main") or soup.find("article") or soup

        # Extract title
        if main.find("h1"):
            title = main.find("h1").get_text(strip=True)
        elif soup.title:
            title = soup.title.string
        else:
            title = ""

        # Extract meta description
        meta = soup.find("meta", {"name": "description"})
        summary = meta["content"].strip() if meta and meta.get("content") else ""

        # Extract paragraph text
        text = " ".join(p.get_text(strip=True) for p in main.find_all("p"))
        if not summary:
            summary = text[:800]

        return title, summary, text

    def _compute_hash(self, text):
        return hashlib.md5(text.encode()).hexdigest()

    def _store_page(self, url, title, summary, content, category, lang):
        h = self._compute_hash(content)
        conn = self._connect_db()
        cur = conn.cursor()

        # Check if content or URL already exists
        cur.execute("SELECT id FROM pages WHERE content_hash=? OR url=?", (h, url))
        if cur.fetchone():
            conn.close()
            return

        cur.execute(
            """INSERT INTO pages(url, title, summary, content, category, language, content_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (url, title, summary, content, category, lang, h)
        )
        conn.commit()
        conn.close()

    def _guess_category(self, url, text):
        u = url.lower() + text.lower()
        if any(k in u for k in ["health", "hospital", "mohfw", "vaccine"]):
            return "health"
        if any(k in u for k in ["agri", "farm", "krishi", "pmkisan"]):
            return "agriculture"
        if any(k in u for k in ["education", "school", "student", "ncert"]):
            return "education"
        return "government"

    def _fetch(self, url):
        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=10)
            resp.raise_for_status()
            return resp.text

        except requests.exceptions.RequestException as e:
            err = str(e).lower()
            if "name or service not known" in err or "getaddrinfo" in err:
                try:
                    host = requests.utils.urlparse(url).hostname
                    ip = socket.gethostbyname(host)
                    new_url = url.replace(host, ip)
                    resp = self.session.get(
                        new_url, headers=self._get_headers(host), timeout=10, verify=False
                    )
                    return resp.text
                except Exception:
                    return None
            return None

    def crawl(self, categories=None, keywords=None, max_pages=None):
        frontier = []
        visited = set()
        limit = max_pages or self.max_pages

        # Load initial seed URLs
        for cat, urls in PRIMARY_SEEDS.items():
            if not categories or cat in categories:
                frontier.extend(urls)

        random.shuffle(frontier)
        stored = []

        while frontier and len(stored) < limit:
            url = frontier.pop(0)
            if url in visited:
                continue

            visited.add(url)
            html = self._fetch(url)
            if not html:
                continue

            title, summary, content = self._clean_text(html)
            if len(content) < 100:
                continue

            category = self._guess_category(url, content)
            lang = "hindi" if "कृषि" in content else "english"

            self._store_page(url, title, summary, content, category, lang)
            stored.append({"url": url, "title": title, "category": category})

            time.sleep(self.politeness)

        return stored
