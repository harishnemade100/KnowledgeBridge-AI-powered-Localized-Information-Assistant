from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import search_route, user_route, crawler_route
from app.utils.logger import setup_logger

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
app.include_router(search_route.APIRouter, prefix="/api/v1", tags=["search"])
app.include_router(user_route.APIRouter, prefix="/api/v1", tags=["users"])
app.include_router(crawler_route.APIRouter, prefix="/api/v1", tags=["crawl"])

@app.get("/")
async def root():
    return {"message": "Rural India Search Engine API", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "search-engine"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)