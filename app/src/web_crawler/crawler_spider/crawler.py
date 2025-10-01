import requests
from bs4 import BeautifulSoup
import time
import hashlib
import sqlite3
from urllib.parse import urljoin, urlparse
from app.src.web_crawler.crawler_spider.seeds import PRIMARY_SEEDS, TRUSTED_SUFFIXES


class Crawler:
    def __init__(self, db_path="storage.db", politeness=1.5, max_pages=200):
        self.db_path = db_path
        self.politeness = politeness
        self.max_pages = max_pages
        self.session = requests.Session()
        self.user_agent = "BharatSearchBot/1.0 (+https://india.gov.in)"
        self.session.headers.update({"User-Agent": self.user_agent})


    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


    def _is_trusted(self, url):
        parsed = urlparse(url)
        host = parsed.netloc
        return any(host.endswith(suf) for suf in TRUSTED_SUFFIXES)


    def _clean_text(self, html)-> tuple[str, str]:

        soup = BeautifulSoup(html, "lxml")

        unwanted_tags = ["script", "style", "noscript", "svg", "meta", "link", "img"]

        for tag in soup.find_all(unwanted_tags):
            tag.extract()

        # Get the title, returning an empty string if none is found
        title_tag = soup.title
        title = title_tag.string.strip() if title_tag and title_tag.string else ""

        # Extract all text, using a space as a separator
        text = soup.get_text(separator=" ")

        # Remove extra whitespace and newline characters
        cleaned_text = " ".join(text.split()).strip()

        return title, cleaned_text

    # stored = self._store_page(seed, title, text, category=self._guess_category(seed))
    def _store_page(self, url, title, content, category=None):
        content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT id FROM pages WHERE content_hash = ?", (content_hash,))
        if cur.fetchone():
            conn.close()
            return False
        cur.execute(
        "INSERT OR IGNORE INTO pages (url, title, content, category, content_hash, last_crawled) VALUES (?,?,?,?,?,datetime('now'))",
        (url, title, content, category or "", content_hash),
        )
        conn.commit()
        conn.close()
        return True


    def run_crawl(self):
        conn = self._connect()
        try:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY, url TEXT UNIQUE, title TEXT, content TEXT, category TEXT, content_hash TEXT, last_crawled TIMESTAMP
        )''')
        conn.commit()
        conn.close()


        frontier = list(PRIMARY_SEEDS)
        seen = set()
        pages_crawled = 0


        for seed in frontier:
            if pages_crawled >= self.max_pages:
                break
            try:
                resp = self.session.get(seed, timeout=20)
                resp.raise_for_status()
            except Exception:
                continue
            if resp.status_code != 200:
                continue

            title, text = self._clean_text(resp.text)
            stored = self._store_page(seed, title, text, category=self._guess_category(seed))
            if stored:
                pages_crawled += 1
                soup = BeautifulSoup(resp.text, "lxml")
                        # Extract and add new links to the frontier
                for a in soup.find_all("a", href=True):
                    href = urljoin(seed, a["href"])
                    if href not in seen and self._is_trusted(href):
                        seen.add(href)
                        frontier.append(href)

            time.sleep(self.politeness)
            if pages_crawled >= self.max_pages:
                break


    def _guess_category(self, url):
        url_low = url.lower()
        if "mohfw" in url_low or "health" in url_low:
            return "health"
        if "agr" in url_low or "agri" in url_low:
            return "agriculture"
        if "edu" in url_low or "ugc" in url_low or "cbse" in url_low or ".ac." in url_low:
            return "education"
        return "government"