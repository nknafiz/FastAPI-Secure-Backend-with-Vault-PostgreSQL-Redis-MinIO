

from __future__ import annotations

import time
import math
import logging
import random
from typing import List, Optional, Tuple, Dict, Any, Callable, Sequence
from datetime import datetime, timedelta

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from src.app.core.db import async_redis, qdrant_client, get_session
from sqlmodel import text

logger = logging.getLogger("recommender")
logger.setLevel(logging.INFO)

# -------- CONFIG ----------
SESSION_KEY_FMT = "user:{user_id}:session"  # list of "<item_id>:<event_type>:<ts>"
SESSION_TTL_SECONDS = 60 * 60 * 24  # keep session lists a day in Redis
QDRANT_COLLECTION = "items"
DEFAULT_VECTOR_SIZE = 768

# scoring weights (tunable)
W_SIM = 0.7
W_POP = 0.2
W_REC = 0.1
W_CONTENT = 0.05  # small boost for matching tags/title
# fusion weights for session vs persistent user vector
SESSION_WEIGHT = 0.7
PERSISTENT_WEIGHT = 0.3

# mmr and exploration
MMR_LAMBDA = 0.7
EPSILON = 0.05  # 5% random exploration

# session weighting alpha (for exponential decay). Higher => recent items dominate.
DEFAULT_ALPHA = 0.8

# --------------------------

# --- basic utilities ---------------------------------------------------
def _cosine_sim(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))

def _vector_add(a: List[float], b: List[float], scale_b: float = 1.0) -> List[float]:
    return [x + scale_b * y for x, y in zip(a, b)]

def _vector_scale(a: List[float], s: float) -> List[float]:
    return [x * s for x in a]

def _vector_zero(dim: int) -> List[float]:
    return [0.0] * dim

# --- Qdrant helpers ---------------------------------------------------
async def ensure_vector_collection(client: QdrantClient, vector_size: int = DEFAULT_VECTOR_SIZE) -> None:
    try:
        client.get_collection(collection_name=QDRANT_COLLECTION)
    except Exception:
        logger.info("Creating qdrant collection %s", QDRANT_COLLECTION)
        client.recreate_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=qmodels.VectorParams(size=vector_size, distance=qmodels.Distance.COSINE),
        )

async def upsert_item_embedding(item_id: int, vector: List[float], payload: Optional[Dict[str, Any]] = None) -> None:
    if qdrant_client is None:
        raise RuntimeError("qdrant_client not initialized")
    point = qmodels.PointStruct(id=item_id, vector=vector, payload=payload or {})
    qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=[point])

# --- Redis session helpers --------------------------------------------
async def log_event(user_id: int, item_id: int, event_type: str = "watch", duration: int = 0) -> None:
    if async_redis is None:
        raise RuntimeError("async_redis not initialized")
    key = SESSION_KEY_FMT.format(user_id=user_id)
    ts = int(time.time())
    payload = f"{item_id}:{event_type}:{ts}"
    await async_redis.lpush(key, payload)
    await async_redis.ltrim(key, 0, 199)
    await async_redis.expire(key, SESSION_TTL_SECONDS)

async def get_recent_session_items(user_id: int, n: int = 50) -> List[Tuple[int, str, int]]:
    if async_redis is None:
        raise RuntimeError("async_redis not initialized")
    key = SESSION_KEY_FMT.format(user_id=user_id)
    rows = await async_redis.lrange(key, 0, n - 1)
    parsed: List[Tuple[int, str, int]] = []
    for r in rows:
        try:
            # make sure bytes/str safe
            if isinstance(r, bytes):
                r = r.decode()
            item_s, event_s, ts_s = r.split(":")
            parsed.append((int(item_s), event_s, int(ts_s)))
        except Exception:
            continue
    return parsed  # newest-first (index 0 = most recent)

# --- persistent user vector helpers -----------------------------------
# expects a table user_profile_vector(user_id int primary key, vector float8[]) optional
async def get_persistent_user_vector(user_id: int) -> Optional[List[float]]:
    """
    Fetch a persisted user vector from Postgres if available.
    Schema expected (optional):
      CREATE TABLE user_profile_vector (user_id int PRIMARY KEY, vector double precision[]);
    Returns None if not present.
    """
    try:
        async with get_session() as session:
            sql = text("SELECT vector FROM user_profile_vector WHERE user_id = :uid")
            res = await session.exec(sql, {"uid": user_id})
            r = res.fetchone()
            if r and r[0] is not None:
                return list(r[0])
    except Exception as e:
        logger.debug("Error fetching persistent user vector: %s", e)
    return None

# --- Build weighted session vector ------------------------------------
async def build_user_vector_weighted_from_events(events: List[Tuple[int, str, int]],
                                                alpha: float = DEFAULT_ALPHA,
                                                max_items: int = 20) -> Optional[List[float]]:
    """
    Build user vector from session events with exponential decay by recency.
    events: list of (item_id, event_type, ts) newest-first
    alpha: decay factor per rank (0 < alpha <= 1). larger alpha -> faster decay.
    """
    if not events:
        return None
    if qdrant_client is None:
        raise RuntimeError("qdrant_client not initialized")

    # take up to max_items most recent
    slice_events = events[:max_items]
    item_ids = [iid for iid, _, _ in slice_events]

    try:
        resp = qdrant_client.retrieve(
            collection_name=QDRANT_COLLECTION,
            ids=item_ids,
            with_payload=False,
            with_vectors=True,
        )
    except Exception as e:
        logger.exception("qdrant retrieve failed: %s", e)
        return None

    # map id -> vector
    id_to_vec: Dict[int, List[float]] = {}
    for p in resp:
        if getattr(p, "vector", None) is not None:
            id_to_vec[int(p.id)] = list(p.vector)

    vectors: List[List[float]] = []
    weights: List[float] = []
    for rank, (iid, et, ts) in enumerate(slice_events):
        vec = id_to_vec.get(iid)
        if vec is None:
            continue
        # exponential decay weight: newest (rank=0) highest weight
        w = math.exp(-alpha * rank)
        vectors.append(vec)
        weights.append(w)

    if not vectors:
        return None

    dim = len(vectors[0])
    acc = [0.0] * dim
    total_w = 0.0
    for v, w in zip(vectors, weights):
        total_w += w
        for i, val in enumerate(v):
            acc[i] += val * w
    if total_w == 0.0:
        return None
    return [x / total_w for x in acc]

# --- Qdrant ANN search with payload -----------------------------------
async def _search_similar(usr_vec: List[float], top_k: int = 50, with_payload: bool = True):
    if qdrant_client is None:
        raise RuntimeError("qdrant_client not initialized")
    if usr_vec is None:
        return []
    try:
        hits = qdrant_client.search(collection_name=QDRANT_COLLECTION, query_vector=usr_vec, limit=top_k, with_payload=with_payload)
    except Exception as e:
        logger.exception("qdrant search failed: %s", e)
        return []
    results: List[Dict[str, Any]] = []
    for h in hits:
        score = getattr(h, "score", None)
        vec = getattr(h, "vector", None)
        payload = getattr(h, "payload", {}) or {}
        item_id = int(h.id)
        if score is None and vec is not None:
            score = _cosine_sim(usr_vec, vec)
        results.append({"id": item_id, "score": float(score or 0.0), "payload": payload, "vector": vec})
    return results

# --- fetch metadata from Postgres ------------------------------------
async def _fetch_item_meta(session, item_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not item_ids:
        return {}
    sql = text(
        "SELECT id, title, category, popularity, extract(epoch FROM created_at) as created_ts "
        "FROM item WHERE id = ANY(:ids)"
    )
    params = {"ids": item_ids}
    res = await session.exec(sql, params)
    rows = res.fetchall()
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        out[int(r[0])] = {
            "title": r[1],
            "category": r[2],
            "popularity": float(r[3] or 0.0),
            "created_ts": float(r[4] or 0.0),
        }
    return out

# --- MMR diversity selection ------------------------------------------
def mmr_select(candidates: List[Tuple[int, float, List[float]]], k: int = 12, lambda_: float = MMR_LAMBDA):
    """
    candidates: list of (item_id, score, vector) where vector used for similarity between picks
    returns chosen item_ids in order
    """
    if not candidates:
        return []
    # clone
    cand = candidates[:]
    selected: List[Tuple[int, float, List[float]]] = []
    while len(selected) < min(k, len(cand)):
        best_idx = None
        best_val = -1e9
        for idx, (iid, sc, vec) in enumerate(cand):
            # relevance = sc
            # diversity = max similarity to already selected
            if not selected:
                div = 0.0
            else:
                div = max(_cosine_sim(vec, svec) for (_, _, svec) in selected)
            val = lambda_ * sc - (1.0 - lambda_) * div
            if val > best_val:
                best_val = val
                best_idx = idx
        if best_idx is None:
            break
        selected.append(cand.pop(best_idx))
    return [iid for (iid, sc, vec) in selected]

# --- fallback popular -----------------------------------------------
async def fallback_popular(top_k: int = 12) -> List[Dict[str, Any]]:
    async with get_session() as session:
        sql = text(
            "SELECT id, title, category, popularity, extract(epoch FROM created_at) as created_ts "
            "FROM item ORDER BY popularity DESC LIMIT :limit"
        )
        res = await session.exec(sql, {"limit": top_k})
        rows = res.fetchall()
        return [
            {"id": int(r[0]), "score": float(r[3] or 0.0), "meta": {"title": r[1], "category": r[2]}}
            for r in rows
        ]

# --- Main recommend -----------------------------------------------
async def recommend(user_id: int,
                    top_k: int = 12,
                    exclude_seen_minutes: int = 60,
                    session_alpha: float = DEFAULT_ALPHA,
                    fusion_weights: Tuple[float, float] = (SESSION_WEIGHT, PERSISTENT_WEIGHT),
                    mmr_lambda: float = MMR_LAMBDA,
                    epsilon: float = EPSILON,
                    sequence_reranker: Optional[Callable[[int, List[int], List[Dict[str, Any]]], List[int]]] = None
                    ) -> List[Dict[str, Any]]:
    """
    Main recommend function.

    sequence_reranker: optional callable signature:
       (user_id:int, session_item_ids:List[int], candidate_dicts:List[dict]) -> ordered_list_of_item_ids
    This allows plugging in a learned sequence model (SASRec/GRU4Rec) for re-ranking.
    """
    # 1) get session events
    session_events = await get_recent_session_items(user_id, n=50)  # newest-first
    recent_item_ids = [iid for (iid, _, _) in session_events]

    # 2) session vector (weighted)
    user_session_vec = await build_user_vector_weighted_from_events(session_events, alpha=session_alpha, max_items=20)

    # 3) persistent user vector (optional)
    persistent_vec = await get_persistent_user_vector(user_id)
    user_vec: Optional[List[float]] = None
    if user_session_vec is not None and persistent_vec is not None:
        w_s, w_p = fusion_weights
        # normalize weights
        s = w_s + w_p
        w_s /= s
        w_p /= s
        user_vec = _vector_add(_vector_scale(user_session_vec, w_s), persistent_vec, scale_b=w_p)
    elif user_session_vec is not None:
        user_vec = user_session_vec
    elif persistent_vec is not None:
        user_vec = persistent_vec
    else:
        user_vec = None

    # cold-start: fallback popular
    if user_vec is None:
        logger.debug("Cold start for user %s", user_id)
        return await fallback_popular(top_k)

    # 4) ANN search
    candidates_raw = await _search_similar(user_vec, top_k=300, with_payload=True)
    if not candidates_raw:
        return await fallback_popular(top_k)

    # build set of recently seen to exclude
    seen_cutoff = int(time.time()) - exclude_seen_minutes * 60
    seen_set = {iid for iid, _, ts in session_events if ts >= seen_cutoff}

    # 5) fetch metadata for candidate ids
    cand_ids = [c["id"] for c in candidates_raw]
    async with get_session() as session:
        meta_map = await _fetch_item_meta(session, cand_ids)

    # 6) compute final scoring per candidate
    now_ts = time.time()
    scored_candidates: List[Tuple[int, float, List[float], Dict[str, Any]]] = []  # id, score, vector, payload/meta
    for c in candidates_raw:
        cid = int(c["id"])
        if cid in seen_set:
            continue
        vec = c.get("vector")
        sim_score = float(c.get("score", 0.0))
        m = meta_map.get(cid, {})
        pop = math.log1p(m.get("popularity", 0.0)) if m else 0.0
        age_hours = (now_ts - m.get("created_ts", now_ts)) / 3600.0 if m else 0.0
        rec_score = 1.0 / (1.0 + 0.02 * age_hours)
        # content-based tiny boost using payload tags/title
        payload = c.get("payload", {}) or {}
        content_boost = 0.0
        # if payload contains tags or title, give small boost if overlap with session items' payloads
        if payload:
            # simple heuristic: boost if tag intersection with session items payloads (requires session payloads stored previously)
            # As a cheap proxy, if payload has 'tags' or 'category' we add a small boost
            if payload.get("tags"):
                content_boost += 0.02 * min(len(payload.get("tags", [])), 3)
            if payload.get("category"):
                content_boost += 0.01

        final_score = W_SIM * sim_score + W_POP * pop + W_REC * rec_score + W_CONTENT * content_boost
        scored_candidates.append((cid, final_score, vec or [], {"meta": m, "payload": payload}))

    if not scored_candidates:
        return await fallback_popular(top_k)

    # 7) optional sequence reranker hook (if provided, it should return an ordered list of ids)
    if sequence_reranker is not None:
        try:
            ordered_ids = sequence_reranker(user_id, recent_item_ids, [{"id": cid, "score": sc, "meta": info["meta"], "payload": info["payload"]} for cid, sc, _, info in scored_candidates])
            # reorder scored_candidates by ordered_ids preserving only those present
            id_to_entry = {cid: (cid, sc, vec, info) for cid, sc, vec, info in scored_candidates}
            new_scored = []
            for oid in ordered_ids:
                if oid in id_to_entry:
                    new_scored.append(id_to_entry[oid])
            # fallback append remaining
            remaining = [e for e in scored_candidates if e[0] not in set(ordered_ids)]
            new_scored.extend(remaining)
            scored_candidates = new_scored
        except Exception as e:
            logger.exception("sequence_reranker failed: %s", e)

    # 8) sort by final_score
    scored_candidates.sort(key=lambda x: x[1], reverse=True)

    # 9) Prepare (id, score, vector) for MMR selection
    mmr_input = [(cid, sc, vec if vec else _vector_zero(len(user_vec))) for cid, sc, vec, _ in scored_candidates]

    # 10) Apply MMR for diversity
    selected_ids = mmr_select(mmr_input, k=top_k, lambda_=mmr_lambda)

    # 11) Epsilon-greedy exploration: with probability epsilon replace one slot with a random popular unseen item
    if epsilon > 0 and random.random() < epsilon:
        async with get_session() as session:
            sql = text("SELECT id FROM item WHERE id != ALL(:seen) ORDER BY popularity DESC LIMIT :limit")
            # build seen list combining selected and recent items
            seen_combined = list(set(selected_ids + recent_item_ids))
            try:
                res = await session.exec(sql, {"seen": seen_combined or [0], "limit": 100})
                rows = res.fetchall()
                popular_candidates = [int(r[0]) for r in rows if int(r[0]) not in seen_combined]
                if popular_candidates:
                    choice = random.choice(popular_candidates)
                    # replace last item with the random popular choice
                    if selected_ids:
                        replaced = selected_ids[-1]
                        selected_ids[-1] = choice
                        logger.debug("Epsilon-greedy replaced %s with %s", replaced, choice)
            except Exception as e:
                logger.debug("epsilon-greedy DB failure: %s", e)

    # 12) Fetch metadata for final set and assemble result
    async with get_session() as session:
        item_meta = await _fetch_item_meta(session, selected_ids)

    results: List[Dict[str, Any]] = []
    for iid in selected_ids:
        # find score and payload from earlier scored list if possible
        found = next((sc for (cid, sc, vec, info) in scored_candidates if cid == iid), None)
        m = item_meta.get(iid, {})
        results.append({"id": iid, "score": float(found or 0.0), "meta": m})

    return results

# --- Example usage/demo helpers ---------------------------------------
async def example_usage():
    await log_event(123, 101, "watch")
    await log_event(123, 102, "watch")
    recs = await recommend(123, top_k=8)
    print("recs:", recs)

# EOF
