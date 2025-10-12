import threading
import time
from app.src.web_crawler.crawler_spider.crawler import EnhancedCrawler  
from app.src.semantic_using_NLP.semantic import build_embeddings

# interval in seconds between automatic refreshes (e.g., 6 hours)
DEFAULT_INTERVAL = 60 * 60 * 6

class AutoRefresher:
    def __init__(self, interval=DEFAULT_INTERVAL, categories=None, keywords=None, max_pages=50):
        self.interval = interval
        self.categories = categories
        self.keywords = keywords
        self.max_pages = max_pages
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self):
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self):
        self._stop.set()

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                crawler = EnhancedCrawler()
                crawler.crawl(categories=self.categories, keywords=self.keywords, max_pages=self.max_pages)
                # rebuild semantic embeddings after crawl
                build_embeddings(force_rebuild=True)
            except Exception as e:
                print(f"[autorefresh] error: {e}")
            # wait
            for _ in range(int(self.interval)):
                if self._stop.is_set():
                    break
                time.sleep(1)
