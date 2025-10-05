from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from app.src.web_crawler.indexer.indexer import RuralSearchEngine

router = APIRouter()
search_engine = RuralSearchEngine()

class UserProfile(BaseModel):
    user_id: str
    preferred_language: str = "hindi"
    preferred_categories: List[str] = []
    location: Optional[str] = None

class SearchHistoryItem(BaseModel):
    query: str
    category: str
    timestamp: str

@router.post("/users/{user_id}/profile")
async def update_user_profile(user_id: str, profile: UserProfile):
    try:
        # In a real application, you'd store this in database
        return {
            "message": "Profile updated successfully",
            "user_id": user_id,
            "profile": profile.dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profile update error: {str(e)}")

@router.get("/users/{user_id}/history")
async def get_user_search_history(user_id: str, limit: int = 20):
    try:
        history = search_engine._get_user_search_history(user_id, limit)
        return {"user_id": user_id, "search_history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"History error: {str(e)}")

@router.get("/users/{user_id}/interests")
async def get_user_interests(user_id: str):
    try:
        interests = search_engine._get_user_interests(user_id)
        return {"user_id": user_id, "interests": interests}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Interests error: {str(e)}")

@router.delete("/users/{user_id}/history")
async def clear_user_history(user_id: str):
    try:
        search_engine._clear_user_history(user_id)
        return {"message": "Search history cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Clear history error: {str(e)}")