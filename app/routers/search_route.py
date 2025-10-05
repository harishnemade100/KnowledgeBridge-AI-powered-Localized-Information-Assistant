from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from app.src.web_crawler.indexer.indexer import RuralSearchEngine

router = APIRouter()
search_engine = RuralSearchEngine()

class SearchRequest(BaseModel):
    query: str
    user_id: str = "default"
    category: Optional[str] = None
    limit: int = 10

class SearchResponse(BaseModel):
    results: List[dict]
    suggestions: List[str]
    category: Optional[str]

class FeedRequest(BaseModel):
    user_id: str
    limit: int = 5

class FeedResponse(BaseModel):
    feed_items: List[dict]
    user_interests: List[str]

@router.post("/search", response_model=SearchResponse)
async def search_content(request: SearchRequest):
    try:
        results = search_engine.search(
            query=request.query,
            user_id=request.user_id,
            category=request.category,
            limit=request.limit
        )
        
        suggestions = search_engine.get_search_suggestions(request.query)
        category = search_engine.indexer.map_query_to_category(request.query)
        
        return SearchResponse(
            results=results,
            suggestions=suggestions,
            category=category
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@router.get("/search/suggestions")
async def get_suggestions(
    query: str = Query(..., min_length=1),
    user_id: str = Query("default")
):
    try:
        suggestions = search_engine.get_search_suggestions(query)
        return {"suggestions": suggestions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Suggestion error: {str(e)}")

@router.post("/feed", response_model=FeedResponse)
async def get_personalized_feed(request: FeedRequest):
    try:
        feed_items = search_engine.get_personalized_feed(
            user_id=request.user_id,
            limit=request.limit
        )
        
        # Get user interests
        user_interests = search_engine._get_user_interests(request.user_id)
        
        return FeedResponse(
            feed_items=feed_items,
            user_interests=user_interests
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feed error: {str(e)}")

@router.get("/stats")
async def get_engine_stats():
    try:
        stats = search_engine.indexer.get_statistics()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")