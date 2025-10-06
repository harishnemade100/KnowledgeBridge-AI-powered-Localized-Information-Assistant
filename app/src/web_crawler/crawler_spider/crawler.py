import requests
import time
import hashlib
import sqlite3
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from app.src.web_crawler.crawler_spider.seeds import PRIMARY_SEEDS

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class EnhancedCrawler:
    def __init__(self, db_path="storage.db", politeness=2.0, max_pages=100, use_selenium=True):
        self.db_path = db_path
        self.politeness = politeness
        self.max_pages = max_pages
        self.use_selenium = use_selenium
        self.session = requests.Session()
        
        # Rotating user agents to avoid blocking
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
        ]
        
        self._init_database()

    def _get_random_user_agent(self):
        return random.choice(self.user_agents)

    def _init_database(self):
        """Initialize database tables"""
        conn = self._connect()
        conn.execute("""CREATE TABLE IF NOT EXISTS pages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE,
                        title TEXT,
                        content TEXT,
                        category TEXT,
                        content_hash TEXT,
                        language TEXT DEFAULT 'english',
                        last_crawled TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
        conn.commit()
        conn.close()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _is_trusted(self, url):
        """Check if URL belongs to trusted domains"""
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            trusted_suffixes = [".gov.in", ".nic.in", ".ac.in", ".org.in", ".edu.in"]
            return any(host.endswith(suffix) for suffix in trusted_suffixes)
        except Exception:
            return False

    def _fetch(self, url):
        """Fetch URL with better error handling and headers"""
        max_retries = 3
        backoff = 2
        
        for attempt in range(max_retries):
            try:
                headers = {
                "User-Agent": self._get_random_user_agent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                }
                
                print(f"Fetching: {url} (Attempt {attempt+1}/{max_retries})")
                response = self.session.get(
                    url, 
                    timeout=15, 
                    verify=False,
                    headers=headers
                )
                
                if response.status_code == 403:
                    print(f"⚠️ 403 Forbidden for {url}, trying with different approach...")
                    return self._fetch_selenium(url) if self.use_selenium else None
                
                response.raise_for_status()
                
                if "text/html" not in response.headers.get("content-type", "").lower():
                    print(f"Skipping non-HTML content: {response.headers.get('content-type')}")
                    return None
                    
                return response.text
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    print(f"⚠️ 403 Forbidden for {url}, using Selenium fallback...")
                    return self._fetch_selenium(url) if self.use_selenium else None
                elif 400 <= e.response.status_code < 500:
                    print(f"⚠️ {url} returned {e.response.status_code}, skipping")
                    return None
            except requests.RequestException as e:
                print(f"Request error: {e}")
                time.sleep(backoff ** attempt)

        return self._fetch_selenium(url) if self.use_selenium else None

    def _fetch_selenium(self, url):
        """Use Selenium for JavaScript-heavy sites or when blocked"""
        try:
            print(f"Using Selenium for: {url}")
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument(f"user-agent={self._get_random_user_agent()}")
            
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(5)  # Wait for JavaScript to load
            html = driver.page_source
            driver.quit()
            return html
        except Exception as e:
            print(f"Selenium error for {url}: {e}")
            return None

    def _clean_text(self, html):
        """Extract clean text from HTML with better content detection"""
        if not html:
            return "", ""
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Remove unwanted tags
        for tag in soup(["script", "style", "noscript", "meta", "link", "header", "footer", "nav"]):
            tag.decompose()
        
        # Try to find main content
        main_content = soup.find('main') or soup.find('article') or soup.find(id='content') or soup
        
        title = main_content.title.string.strip() if main_content.title and main_content.title.string else ""
        if not title:
            h1 = main_content.find('h1')
            title = h1.get_text().strip() if h1 else ""
        
        # Get text with better formatting
        text = ' '.join(main_content.get_text(separator=" ", strip=True).split())
        return title, text

    def _detect_language(self, text):
        """Simple language detection for Indian languages"""
        hindi_keywords = ['की', 'के', 'है', 'में', 'को', 'यह', 'वह', 'और']
        if any(keyword in text for keyword in hindi_keywords):
            return 'hindi'
        return 'english'

    def run_crawl(self, categories=None, keywords=None):
        """Enhanced crawling with better content filtering"""
        
        stored_pages = []
        frontier = []
        visited = set()
        pages_crawled = 0

        # Build initial frontier
        if categories:
            for category in categories:
                frontier.extend(PRIMARY_SEEDS.get(category, []))
        else:
            for urls in PRIMARY_SEEDS.values():
                frontier.extend(urls)
        
        frontier = list(set(frontier))
        print(f"🚀 Starting crawl with {len(frontier)} seed URLs...")

        while frontier and pages_crawled < self.max_pages:
            url = frontier.pop(0)
            
            if url in visited:
                continue
            visited.add(url)

            html = self._fetch(url)
            if not html:
                continue

            title, text = self._clean_text(html)
            
            # Better content validation
            if not text or len(text.strip()) < 100:
                print(f"⚠️ Skipping {url} - insufficient content")
                continue

            category = self._guess_category(url, text)
            language = self._detect_language(text)
            
            if categories and category not in categories:
                print(f"⚠️ Skipping {url} - category {category} not in {categories}")
                continue

            # Store page
            if self._store_page(url, title, text, category, language):
                stored_pages.append({
                    "url": url, 
                    "title": title, 
                    "category": category, 
                    "language": language,
                    "content_length": len(text)
                })
                pages_crawled += 1
                print(f"✅ [{pages_crawled}] {url} ({category}, {language}) - {len(text)} chars")

            # Extract links with better filtering
            try:
                soup = BeautifulSoup(html, "html.parser")
                for anchor in soup.find_all("a", href=True):
                    href = anchor["href"]
                    absolute_url = urljoin(url, href)
                    
                    if (self._is_trusted(absolute_url) and 
                        absolute_url not in visited and 
                        absolute_url not in frontier):
                        frontier.append(absolute_url)
                        
            except Exception as e:
                print(f"Error extracting links: {e}")

            time.sleep(self.politeness + random.uniform(0, 1))  # Random delay

        print(f"🎉 Crawl completed. Stored {len(stored_pages)} pages.")
        return stored_pages

    def _guess_category(self, url, text=None):
        """Enhanced category guessing using URL and content"""
        url_lower = url.lower()
        text_lower = text.lower() if text else ""
        
        health_keywords = ['health', 'medical', 'hospital', 'covid', 'vaccine', 'doctor', 'corona', 'स्वास्थ्य', 'चिकित्सा']
        agriculture_keywords = ['agriculture', 'farm', 'crop', 'krishi', 'farmer', 'soil', 'कृषि', 'फसल', 'किसान']
        education_keywords = ['education', 'school', 'college', 'exam', 'student', 'teacher', 'शिक्षा', 'विद्यालय', 'छात्र']
        
        # Check URL first
        if any(keyword in url_lower for keyword in health_keywords):
            return "health"
        elif any(keyword in url_lower for keyword in agriculture_keywords):
            return "agriculture" 
        elif any(keyword in url_lower for keyword in education_keywords):
            return "education"
        
        # Check content if URL is ambiguous
        if text:
            if any(keyword in text_lower for keyword in health_keywords):
                return "health"
            elif any(keyword in text_lower for keyword in agriculture_keywords):
                return "agriculture"
            elif any(keyword in text_lower for keyword in education_keywords):
                return "education"
        
        return "government"

    def _store_page(self, url, title, content, category=None, language='english'):
        """Store page in database"""
        try:
            content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
            conn = self._connect()
            cur = conn.cursor()
            
            # Check duplicate
            cur.execute("SELECT id FROM pages WHERE content_hash=?", (content_hash,))
            if cur.fetchone():
                conn.close()
                return False
                
            cur.execute("""INSERT OR REPLACE INTO pages 
                          (url, title, content, category, content_hash, language) 
                          VALUES (?, ?, ?, ?, ?, ?)""", 
                        (url, title, content, category or "", content_hash, language))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error storing page {url}: {e}")
            return False