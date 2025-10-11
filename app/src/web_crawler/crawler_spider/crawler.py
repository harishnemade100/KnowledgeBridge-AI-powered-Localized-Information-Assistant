# crawler.py
import random
import time
import socket
import hashlib
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from seeds import PRIMARY_SEEDS, TRUSTED_SUFFIXES
from app.src.web_crawler.indexer.indexer import db_connect

DEFAULT_TIMEOUT = 12
CRAWL_POLITENESS = 1.0
MIN_CONTENT_LENGTH = 120  # minimum chars to consider storing

class EnhancedCrawler:
    def __init__(self, politeness=CRAWL_POLITENESS, max_pages=200):
        self.politeness = politeness
        self.max_pages = max_pages
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1,
                        status_forcelist=(500,502,503,504),
                        allowed_methods=frozenset(["GET","POST"]))
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/92.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15"
        ]

    def _get_headers(self, host=None):
        h = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }
        if host:
            h["Host"] = host
        return h

    def _compute_hash(self, text: str) -> str:
        return hashlib.md5((text or "").encode("utf-8")).hexdigest()

    def _clean_text(self, html: str):
        if not html:
            return "", "", ""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script","style","noscript","header","footer","nav","form"]):
            tag.decompose()
        main = soup.find("main") or soup.find("article") or soup
        # title
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        h1 = main.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(strip=True)
        # meta description
        meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
        summary = meta.get("content").strip() if meta and meta.get("content") else ""
        paragraphs = [p.get_text(separator=" ", strip=True) for p in main.find_all("p") if p.get_text(strip=True)]
        content = " ".join(paragraphs)
        if not content:
            content = main.get_text(separator=" ", strip=True)
        content = " ".join(content.split())
        if not summary:
            summary = content[:500]
        return title.strip(), summary.strip(), content.strip()

    def _detect_language(self, text: str):
        hindi_keywords = ['की','के','है','में','यह','और','नए','किसान']
        if any(k in text for k in hindi_keywords):
            return "hindi"
        return "english"

    def _guess_category(self, url: str, text: str):
        urll = url.lower() + (text or "").lower()
        health_k = ['health','hospital','mohfw','vaccine','covid','corona','coronavirus']
        agri_k = ['agri','farm','krishi','कृषि','pmkisan','farmer']
        edu_k = ['education','school','ugc','ncert','student','college']
        if any(k in urll for k in health_k):
            return "health"
        if any(k in urll for k in agri_k):
            return "agriculture"
        if any(k in urll for k in edu_k):
            return "education"
        return "government"

    def _fetch(self, url: str) -> Optional[str]:
        headers = self._get_headers()
        try:
            resp = self.session.get(url, timeout=DEFAULT_TIMEOUT, headers=headers)
            resp.raise_for_status()
            if "text/html" not in resp.headers.get("content-type", ""):
                return None
            return resp.text
        except requests.exceptions.RequestException as e:
            err = str(e).lower()
            if "name or service not known" in err or "getaddrinfo" in err or "temporary failure in name resolution" in err:
                try:
                    host = requests.utils.urlparse(url).hostname
                    ip = socket.gethostbyname(host)
                    parsed = requests.utils.urlparse(url)
                    scheme = parsed.scheme
                    port = f":{parsed.port}" if parsed.port else ""
                    path = parsed.path or "/"
                    if parsed.query:
                        path += "?" + parsed.query
                    ip_url = f"{scheme}://{ip}{port}{path}"
                    headers = self._get_headers(host=host)
                    resp = self.session.get(ip_url, timeout=DEFAULT_TIMEOUT, headers=headers, verify=True)
                    resp.raise_for_status()
                    if "text/html" not in resp.headers.get("content-type",""):
                        return None
                    return resp.text
                except Exception:
                    return None
            return None

    def _store_page(self, url: str, title: str, summary: str, content: str, category: str, language: str):
        conn = db_connect()
        cur = conn.cursor()
        content_hash = self._compute_hash(content or summary or title or url)
        # dedupe by hash or url
        cur.execute("SELECT id FROM pages WHERE content_hash = ? OR url = ?", (content_hash, url))
        if cur.fetchone():
            conn.close()
            return False
        cur.execute(
            """INSERT INTO pages (url, title, summary, content, category, language, content_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (url, title, summary, content, category, language, content_hash)
        )
        conn.commit()
        conn.close()
        return True

    def crawl(self, categories: Optional[List[str]] = None, keywords: Optional[List[str]] = None, max_pages: Optional[int] = None):
        # keywords: matches anywhere in content/title/summary (case-insensitive)
        kw_lower = [k.lower() for k in (keywords or []) if k]
        frontier = []
        visited = set()
        stored = []
        limit = max_pages or self.max_pages
        if categories:
            for c in categories:
                frontier.extend(PRIMARY_SEEDS.get(c, []))
        else:
            for urls in PRIMARY_SEEDS.values():
                frontier.extend(urls)
        random.shuffle(frontier)
        while frontier and len(stored) < limit:
            url = frontier.pop(0)
            if url in visited:
                continue
            visited.add(url)
            html = self._fetch(url)
            if not html:
                continue
            title, summary, content = self._clean_text(html)
            text_for_check = " ".join([title, summary, content]).lower()
            # filter by keywords if given
            if kw_lower:
                if not any(k in text_for_check for k in kw_lower):
                    # skip storing but still expand frontier
                    pass_store = False
                else:
                    pass_store = True
            else:
                pass_store = True
            if not content or len(content) < MIN_CONTENT_LENGTH:
                continue
            category = self._guess_category(url, content)
            language = self._detect_language(content)
            if pass_store:
                ok = self._store_page(url, title, summary, content, category, language)
                if ok:
                    stored.append({"url": url, "title": title, "category": category})
            # expand frontier with trusted links found on this page
            try:
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    abs_url = requests.compat.urljoin(url, href)
                    if any(abs_url.lower().endswith(s) for s in [".pdf",".jpg",".png",".zip",".doc",".docx"]):
                        continue
                    parsed = requests.utils.urlparse(abs_url)
                    host = parsed.hostname or ""
                    if host and any(host.endswith(s) for s in TRUSTED_SUFFIXES) and abs_url not in visited:
                        frontier.append(abs_url)
            except Exception:
                pass
            time.sleep(self.politeness + random.random()*0.5)
        return stored
