from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import search_route, user_route, crawler_route
from app.utils.logger import setup_logger
from app.src.web_crawler.indexer.indexer import Indexer

search_engine = Indexer()
search_engine.ensure_tables()

# Setup logger
logger = setup_logger()

app = FastAPI(
    title="Rural India Search Engine",
    description="A personalized search engine for rural Indian users",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(search_route.router, prefix="/api/v1", tags=["search"])
app.include_router(user_route.router, prefix="/api/v1", tags=["users"])
app.include_router(crawler_route.router, prefix="/api/v1", tags=["crawl"])

@app.get("/")
async def root():
    return {"message": "Rural India Search Engine API", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "search-engine"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)