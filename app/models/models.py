from pydantic import BaseModel
from typing import List, Optional

class RegisterModel(BaseModel):
    username: str
    password: str

class CrawlRequest(BaseModel):
    categories: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    max_pages: Optional[int] = None

class SearchResponseItem(BaseModel):
    url: str
    title: str
    summary: str
    category: str
    language: str
    score: Optional[float] = None
