# crawler/crawler.py
import os 
import requests
from bs4 import BeautifulSoup
import sqlite3
import hashlib
import time
from urllib.parse import urljoin, urlparse
from seeds import TRUSTED_SEEDS


script_dir = os.path.dirname(os.path.abspath(__file__))

project_root = os.path.dirname(script_dir)

DB_PATH = os.path.join(project_root, "crawler_spider", "storage.db")


def clean_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script", "style", "noscript"]):
        s.extract()
    return soup.get_text(" ", strip=True)

def categorize(url):
    if "gov" in url:
        return "government"
    elif "edu" in url:
        return "education"
    elif "agri" in url:
        return "agriculture"
    elif "health" in url or "mohfw" in url:
        return "health"
    return "general"

def crawl(seed_urls, max_pages=30):
    visited = set()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for seed in seed_urls:
        queue = [seed]
        while queue and len(visited) < max_pages:
            url = queue.pop(0)
            if url in visited: 
                continue
            try:
                r = requests.get(url, timeout=5)
                if r.status_code != 200:
                    continue
                visited.add(url)

                text = clean_text(r.text)
                title = BeautifulSoup(r.text, "html.parser").title.string if BeautifulSoup(r.text, "html.parser").title else ""
                content_hash = hashlib.md5(text.encode()).hexdigest()
                category = categorize(url)

                cur.execute("INSERT OR IGNORE INTO pages (url, title, content, category, content_hash) VALUES (?, ?, ?, ?, ?)",
                            (url, title, text, category, content_hash))
                conn.commit()

                # extract new links
                soup = BeautifulSoup(r.text, "html.parser")
                for link in soup.find_all("a", href=True):
                    new_url = urljoin(url, link["href"])
                    if urlparse(new_url).netloc.endswith(urlparse(seed).netloc):
                        queue.append(new_url)

                time.sleep(1)  # polite crawling

            except Exception as e:
                print(f"Error crawling {url}: {e}")
    conn.close()

if __name__ == "__main__":
    crawl(TRUSTED_SEEDS, max_pages=50)
