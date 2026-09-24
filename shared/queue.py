"""
shared/queue.py — Bounded Redis queue with DLQ semantics (§33)

Queues:
  tgq:meta_capi        — Meta CAPI jobs (MetaEvent ids)
  tgq:telegram_events  — normalized event jobs (future)
  tgq:automation       — flow runs (future)
  tgq:analytics        — rollups (future)

Jobs are JSON blobs with {id, type, payload, attempts, created_at}.
Workers pop with BRPOP and handle retry/backoff via a delayed zset.
"""
from __future__ import annotations

import json
import time
import uuid
import logging
from typing import Any

import redis.asyncio as aioredis

from shared.config import get_settings

logger = logging.getLogger(__name__)


def get_redis() -> aioredis.Redis:
    settings = get_settings()
    return aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)


async def enqueue(queue: str, payload: dict[str, Any], *, maxlen: int | None = None) -> str:
    settings = get_settings()
    job_id = payload.get("job_id") or str(uuid.uuid4())
    job = {"job_id": job_id, "payload": payload, "attempts": 0, "created_at": time.time()}
    r = get_redis()
    try:
        # bounded queue — prevents unbounded growth if worker is down
        ml = maxlen or settings.QUEUE_MAXLEN
        # Use LPUSH + LTRIM to bound
        await r.lpush(queue, json.dumps(job))
        await r.ltrim(queue, 0, ml - 1)
        # check if we trimmed (queue was full)
        llen = await r.llen(queue)
        if llen >= ml:
            logger.warning("QUEUE_NEAR_FULL queue=%s llen=%d maxlen=%d", queue, llen, ml)
        return job_id
    finally:
        await r.aclose()


async def dequeue(queue: str, timeout: int = 5) -> dict[str, Any] | None:
    """Blocking pop — returns job dict or None on timeout."""
    r = get_redis()
    try:
        item = await r.brpop(queue, timeout=timeout)
        if not item:
            return None
        _, raw = item
        return json.loads(raw)
    finally:
        await r.aclose()


async def requeue_delayed(queue: str, job: dict[str, Any], delay_seconds: int):
    """Put job into delayed zset to be retried later."""
    settings = get_settings()
    delayed_key = queue + settings.QUEUE_DELAYED_SUFFIX
    r = get_redis()
    try:
        job["attempts"] = job.get("attempts", 0) + 1
        job["next_attempt_at"] = time.time() + delay_seconds
        await r.zadd(delayed_key, {json.dumps(job): job["next_attempt_at"]})
    finally:
        await r.aclose()


async def promote_delayed(queue: str, batch: int = 20) -> int:
    """Move due delayed jobs back to main queue. Returns count promoted."""
    settings = get_settings()
    delayed_key = queue + settings.QUEUE_DELAYED_SUFFIX
    r = get_redis()
    try:
        now = time.time()
        due = await r.zrangebyscore(delayed_key, 0, now, start=0, num=batch)
        if not due:
            return 0
        pipe = r.pipeline()
        for raw in due:
            pipe.lpush(queue, raw)
            pipe.zrem(delayed_key, raw)
        await pipe.execute()
        return len(due)
    finally:
        await r.aclose()


async def queue_depth(queue: str) -> int:
    r = get_redis()
    try:
        return await r.llen(queue)
    finally:
        await r.aclose()


async def delayed_depth(queue: str) -> int:
    settings = get_settings()
    r = get_redis()
    try:
        return await r.zcard(queue + settings.QUEUE_DELAYED_SUFFIX)
    finally:
        await r.aclose()
