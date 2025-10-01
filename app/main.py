from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from app.src.web_crawler.crawler_spider.crawler import Crawler
from app.src.web_crawler.indexer.indexer import Indexer
# from app.src.recommender.recommender import Recommender
import threading
import time
import os


app = FastAPI(title="Bharat Search Engine")
script_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(script_dir, "src","web_crawler", "crawler_spider", "storage.db")


# Singletons
indexer = Indexer(db_path=DB_PATH)
# recommender = Recommender(db_path=DB_PATH)
crawler = Crawler(db_path=DB_PATH)


# Helper: ensure DB/index exists
indexer.ensure_tables()
# recommender.ensure_tables()


@app.get("/")
def root():
    return {"message": "Bharat Search Engine — hit /search?q=..."}


@app.get("/health")
def health():
    return {"status": "ok", "has_data": indexer.has_pages()}


@app.get("/search")
def search(q: str = Query(..., min_length=1), category: str = Query(None)):
# log query
    indexer.log_search(q)


    # If no data, trigger crawler synchronously (auto-init). This is simple and transparent.
    if not indexer.has_pages():
    # start crawler in a thread but wait until some pages are available or timeout
        crawl_thread = threading.Thread(target=crawler.run_crawl(), kwargs={})
        crawl_thread.start()
    # Return a message indicating crawling started and ask client to retry in a few seconds.
    # For UX, we'll block up to 45 seconds to try to return results after initial crawl progress.
    timeout = 45
    waited = 0
    while waited < timeout:
        if indexer.has_pages():
            break
        time.sleep(1)
        waited += 1
        # proceed whether or not pages appear


    results = indexer.search(q, category=category, limit=20)
    # feed reading history sample (no click info yet)
    return JSONResponse({"query": q, "results": results})


# @app.get("/recommend")
# def recommend(user_id: str = Query(None)):
# # for simplicity: user_id optional; uses aggregate trending if not provided
#     feed = recommender.recommend(user_id=user_id)
#     return {"recommendations": feed}


@app.get("/popular")
def popular(limit: int = 10):
    return {"popular": indexer.get_popular_searches(limit=limit)}