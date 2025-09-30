# api/app.py
from fastapi import FastAPI, Query
from app.src.web_crawler.indexer.indexer import search
from app.src.recommender.recommender import Recommender

app = FastAPI()
recommender = Recommender()

@app.get("/search")
def search_api(q: str = Query(..., description="Search query")):
    results = search(q)
    recommender.log_query(q)
    return {"query": q, "results": results}

@app.get("/recommend")
def recommend_api():
    recs = recommender.recommend()
    return {"personalized_feed": recs}
