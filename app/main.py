# main.py
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.security import OAuth2PasswordRequestForm
from typing import List, Optional
from datetime import datetime
from database import init_db, db_connect
from models import RegisterModel, CrawlRequest, SearchResponseItem
from auth import create_jwt, hash_password, verify_password
from crawler import EnhancedCrawler
from utils import extract_faqs
from semantic import build_embeddings, semantic_rank
from scheduler import AutoRefresher

app = FastAPI(title="KnowledgeBridge - Crawler + Semantic Search API")

# start DB and embeddings on startup
@app.on_event("startup")
def startup_event():
    init_db()
    # initial build of embeddings (empty ok)
    build_embeddings()
    # start auto refresher with default config (optional: adjust categories/keywords)
    global _autoref
    _autoref = AutoRefresher(interval=60*60*6)  # every 6 hours
    _autoref.start()

@app.on_event("shutdown")
def shutdown_event():
    try:
        _autoref.stop()
    except Exception:
        pass

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
    # rebuild semantic embeddings in background
    background.add_task(build_embeddings, True)
    return stored

@app.get("/search", response_model=List[SearchResponseItem])
def search(q: str = Query(..., min_length=1), category: Optional[str] = None, lang: Optional[str] = None, limit: int = 20):
    """
    Combined search:
    - Use SQLite FTS5 to get candidate doc ids matching query (fast)
    - Use semantic TF-IDF re-ranking among candidates to produce relevance score
    """
    conn = db_connect()
    cur = conn.cursor()
    # Use FTS5 MATCH to find candidates
    fts_query = q.replace('"', ' ')  # basic sanitize
    sql = """
    SELECT p.id, p.url, p.title, p.summary, p.category, p.language
    FROM pages_fts f
    JOIN pages p ON f.rowid = p.id
    WHERE pages_fts MATCH ?
    """
    params = [fts_query + "*"]
    if category:
        sql += " AND p.category = ?"
        params.append(category)
    if lang:
        sql += " AND p.language = ?"
        params.append(lang)
    sql += " LIMIT ?"
    params.append(limit * 5)  # get more candidates to re-rank semantically
    cur.execute(sql, params)
    rows = cur.fetchall()
    candidate_ids = [r["id"] for r in rows]
    # If no candidates from FTS, fallback to simple LIKE search
    if not candidate_ids:
        like_q = f"%{q}%"
        sql2 = "SELECT id, url, title, summary, category, language FROM pages WHERE (title LIKE ? OR summary LIKE ? OR content LIKE ?)"
        params2 = [like_q, like_q, like_q]
        if category:
            sql2 += " AND category = ?"
            params2.append(category)
        if lang:
            sql2 += " AND language = ?"
            params2.append(lang)
        sql2 += " LIMIT ?"
        params2.append(limit * 5)
        cur.execute(sql2, params2)
        rows = cur.fetchall()
        candidate_ids = [r["id"] for r in rows]
    # If still no candidates, return empty list
    if not candidate_ids:
        conn.close()
        return []
    # Semantic re-ranking
    scored = semantic_rank(q, candidate_ids, top_k=limit)
    # build response items preserving order by score
    ordered_ids = [doc_id for doc_id, score in scored]
    # fetch details for ordered ids
    placeholders = ",".join("?" for _ in ordered_ids)
    cur.execute(f"SELECT id, url, title, summary, category, language FROM pages WHERE id IN ({placeholders})", ordered_ids)
    docs = {r["id"]: r for r in cur.fetchall()}
    results = []
    for doc_id, score in scored:
        r = docs.get(doc_id)
        if not r:
            continue
        results.append({
            "url": r["url"],
            "title": r["title"] or "",
            "summary": r["summary"] or "",
            "category": r["category"] or "",
            "language": r["language"] or "english",
            "score": round(score, 6)
        })
    conn.close()
    return results

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
    out = []
    for r in rows:
        content = r["content"] or ""
        faqs = extract_faqs(content)
        out.append({
            "url": r["url"],
            "title": r["title"],
            "summary": r["summary"],
            "language": r.get("language", "english"),
            "faqs": faqs
        })
    return {"count": len(out), "data": out}

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}
