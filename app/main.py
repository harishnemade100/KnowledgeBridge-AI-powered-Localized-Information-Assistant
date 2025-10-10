from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from typing import List, Optional
from datetime import datetime

from app.src.web_crawler.crawler_spider.crawler import EnhancedCrawler
from app.src.web_crawler.indexer.indexer import init_db, db_connect
from app.models.models import RegisterModel, CrawlRequest, SearchResponseItem
from app.src.auth.auth import create_jwt, verify_password, hash_password
from app.utils.utils import extract_faqs

app = FastAPI(title="KnowledgeBridge - Crawler + Search API")

@app.on_event("startup")
def startup_event():
    init_db()

@app.post("/auth/register")
def register(payload: RegisterModel):
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                    (payload.username, hash_password(payload.password)))
        conn.commit()
        conn.close()
        return {"ok": True, "msg": "User created"}
    except Exception:
        conn.close()
        raise HTTPException(400, "Username already exists")

@app.post("/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT id, password_hash FROM users WHERE username=?", (form.username,))
    row = cur.fetchone()
    conn.close()
    if not row or not verify_password(form.password, row["password_hash"]):
        raise HTTPException(401, "Invalid credentials")
    token = create_jwt({"sub": form.username})
    return {"access_token": token, "token_type": "bearer"}

@app.post("/crawl/start", response_model=List[dict])
def start_crawl(req: CrawlRequest, background: BackgroundTasks):
    crawler = EnhancedCrawler()
    stored = crawler.crawl(categories=req.categories, keywords=req.keywords, max_pages=req.max_pages)
    return stored

@app.get("/search", response_model=List[SearchResponseItem])
def search(q: str, category: Optional[str] = None, lang: Optional[str] = None, limit: int = 20):
    conn = db_connect()
    cur = conn.cursor()
    like_q = f"%{q}%"
    sql = "SELECT url, title, summary, category, language FROM pages WHERE (title LIKE ? OR summary LIKE ? OR content LIKE ?)"
    params = [like_q, like_q, like_q]
    if category:
        sql += " AND category = ?"
        params.append(category)
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [{"url": r["url"], "title": r["title"], "summary": r["summary"],
             "category": r["category"], "language": r["language"]} for r in rows]

@app.get("/cache/export")
def export_cache(category: Optional[str] = None, limit: int = 200):
    conn = db_connect()
    cur = conn.cursor()
    if category:
        cur.execute("SELECT url, title, summary, content, language FROM pages WHERE category = ? ORDER BY last_crawled DESC LIMIT ?",
                    (category, limit))
    else:
        cur.execute("SELECT url, title, summary, content, category, language FROM pages ORDER BY last_crawled DESC LIMIT ?",
                    (limit,))
    rows = cur.fetchall()
    conn.close()
    data = []
    for r in rows:
        faqs = extract_faqs(r["content"])
        data.append({
            "url": r["url"],
            "title": r["title"],
            "summary": r["summary"],
            "language": r["language"],
            "faqs": faqs
        })
    return {"count": len(data), "data": data}

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}
