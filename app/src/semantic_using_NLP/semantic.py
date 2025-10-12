from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import joblib
import os
from database import db_connect

EMBED_PATH = "embeddings.joblib"

def build_embeddings(force_rebuild=False):
    """
    Build TF-IDF vectorizer and matrix for all documents in DB.
    Persist to EMBED_PATH. Returns (vectorizer, matrix, doc_ids).
    """
    if os.path.exists(EMBED_PATH) and not force_rebuild:
        try:
            data = joblib.load(EMBED_PATH)
            return data["vectorizer"], data["matrix"], data["doc_ids"]
        except Exception:
            pass
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT id, title, summary, content FROM pages ORDER BY id")
    rows = cur.fetchall()
    texts = []
    doc_ids = []
    for r in rows:
        doc_ids.append(r["id"])
        # combine title+summary+content to produce embedding
        texts.append(" ".join([r["title"] or "", r["summary"] or "", r["content"] or ""]))
    conn.close()
    if not texts:
        vectorizer = TfidfVectorizer(stop_words='english', max_features=20000)
        matrix = vectorizer.fit_transform([""])  # empty matrix fallback
        joblib.dump({"vectorizer": vectorizer, "matrix": matrix, "doc_ids": doc_ids}, EMBED_PATH)
        return vectorizer, matrix, doc_ids
    vectorizer = TfidfVectorizer(stop_words='english', max_features=20000)
    matrix = vectorizer.fit_transform(texts)
    joblib.dump({"vectorizer": vectorizer, "matrix": matrix, "doc_ids": doc_ids}, EMBED_PATH)
    return vectorizer, matrix, doc_ids

def semantic_rank(query: str, candidate_doc_ids: list, top_k=10):
    """
    Re-rank candidate_doc_ids (list of page ids) given a query string.
    Returns list of tuples (doc_id, score) sorted by descending score.
    """
    if not candidate_doc_ids:
        return []
    vectorizer, matrix, doc_ids = build_embeddings()
    # Build a mapping from doc_id -> row index in matrix
    id_to_idx = {doc_id: idx for idx, doc_id in enumerate(doc_ids)}
    # Identify indices for candidates (skip if not in id_to_idx)
    cand_indices = [id_to_idx[d] for d in candidate_doc_ids if d in id_to_idx]
    if not cand_indices:
        # fallback: return empty score list
        return []
    # transform query
    qv = vectorizer.transform([query])
    # compute similarities (we only compare against candidate rows)
    cand_matrix = matrix[cand_indices]
    sims = cosine_similarity(qv, cand_matrix)[0]  # shape (len(cand_indices),)
    scored = list(zip([candidate_doc_ids[i] for i in range(len(candidate_doc_ids)) if candidate_doc_ids[i] in id_to_idx], sims))
    # Note: need to map properly; above line matches ordering of cand_indices to doc ids
    # But safer rebuild correct mapping:
    scored = []
    for idx, row_index in enumerate(cand_indices):
        doc_id = doc_ids[row_index]
        score = sims[idx]
        scored.append((doc_id, float(score)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_k]
