from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from app.src.web_crawler.crawler_spider.crawler import EnhancedCrawler
from app.src.web_crawler.indexer.indexer import RuralSearchEngine

router = APIRouter()
search_engine = RuralSearchEngine()

class CrawlRequest(BaseModel):
    categories: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    max_pages: int = 50

class CrawlResponse(BaseModel):
    task_id: str
    status: str
    message: str

# In-memory storage for crawl tasks (use Redis in production)
crawl_tasks = {}

@router.post("/crawl/start", response_model=CrawlResponse)
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    try:
        task_id = f"crawl_{len(crawl_tasks) + 1}"
        
        crawl_tasks[task_id] = {
            "status": "running",
            "pages_crawled": 0,
            "categories": request.categories
        }
        
        background_tasks.add_task(
            run_crawl_task,
            task_id,
            request.categories,
            request.keywords,
            request.max_pages
        )
        
        return CrawlResponse(
            task_id=task_id,
            status="started",
            message=f"Crawl task started for categories: {request.categories}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Crawl start error: {str(e)}")

@router.get("/crawl/status/{task_id}")
async def get_crawl_status(task_id: str):
    try:
        task = crawl_tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return {
            "task_id": task_id,
            "status": task["status"],
            "pages_crawled": task.get("pages_crawled", 0),
            "categories": task.get("categories", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status check error: {str(e)}")

@router.get("/crawl/tasks")
async def get_all_crawl_tasks():
    try:
        return {"tasks": crawl_tasks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tasks fetch error: {str(e)}")

async def run_crawl_task(task_id: str, categories: List[str], keywords: List[str], max_pages: int):
    """Background task to run crawl"""
    try:
        crawler = EnhancedCrawler(max_pages=max_pages)
        results = crawler.run_crawl(categories=categories, keywords=keywords)
        
        # Rebuild index after crawl
        search_engine.indexer._build_inverted_index()
        
        crawl_tasks[task_id].update({
            "status": "completed",
            "pages_crawled": len(results),
            "results": results
        })
        
    except Exception as e:
        crawl_tasks[task_id].update({
            "status": "failed",
            "error": str(e)
        })