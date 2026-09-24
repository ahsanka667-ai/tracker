"""
service_b/capi_worker.py — Meta CAPI queue consumer (§18)

- Pops MetaEvent jobs from Redis
- Sends to Meta Graph API with retry/backoff
- Updates MetaEvent status, http_status, fbtrace_id, error_message
- Handles dead-letter after max attempts
- Heartbeats to Redis so health checks know it's alive

Never called from Telegram handlers directly — they only enqueue.
"""
import asyncio, json, logging, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timezone, timedelta
import aiohttp
from sqlalchemy import select, update

from shared.config import get_settings, validate_or_exit
from shared.logging_config import setup_logging, log_event
from shared.database import AsyncSessionLocal, init_db, bind_ts
from shared.models import MetaEvent, MetaEventStatus

logger = logging.getLogger(__name__)
settings = get_settings()

META_TIMEOUT = settings.META_TIMEOUT_SECONDS
MAX_ATTEMPTS = settings.CAPI_MAX_ATTEMPTS

async def send_to_meta(meta: MetaEvent, pixel_token: str) -> dict:
    """Single attempt to send one MetaEvent to Graph API."""
    from shared.security import decrypt_secret
    # decrypt token if needed
    token = decrypt_secret(pixel_token, settings.ENCRYPTION_KEY) if pixel_token else pixel_token
    if not token:
        return {"error": "missing_access_token", "status": 400}

    # Build payload
    payload = {
        "data": [{
            "event_name": meta.event_name,
            "event_time": meta.event_time,
            "event_id": meta.event_id,
            "event_source_url": meta.event_source_url or "https://t.me",
            "action_source": meta.action_source or "website",
            "user_data": json.loads(meta.user_data) if meta.user_data else {},
        }],
        "access_token": token,
    }
    if meta.custom_data:
        try:
            cd = json.loads(meta.custom_data)
            if cd:
                payload["data"][0]["custom_data"] = cd
        except Exception:
            pass
    if meta.test_event_code:
        payload["test_event_code"] = meta.test_event_code

    url = f"https://graph.facebook.com/{settings.META_GRAPH_VERSION}/{meta.pixel_id}/events"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=META_TIMEOUT)) as session:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                data = await resp.json()
                return {"status": resp.status, "data": data}
    except asyncio.TimeoutError as e:
        return {"error": f"timeout after {META_TIMEOUT}s", "status": 0, "exception": str(e)}
    except aiohttp.ClientConnectorError as e:
        return {"error": f"connection_error: {e}", "status": 0, "exception": str(e)}
    except Exception as e:
        return {"error": str(e), "status": 0, "exception": str(e)}

def is_retryable(status: int, error: str | None) -> bool:
    if status == 0:  # network
        return True
    if 500 <= status < 600:
        return True
    if status == 429:
        return True
    # 400, 401, 403 etc are not retryable (bad token, bad pixel)
    return False

def backoff_seconds(attempt: int) -> int:
    base = settings.CAPI_BACKOFF_BASE_SECONDS
    # exponential: 15, 30, 60, 120, 240, 480...
    delay = base * (2 ** (attempt - 1))
    return min(delay, settings.CAPI_BACKOFF_MAX_SECONDS)

async def process_one(meta_event_id: int):
    async with AsyncSessionLocal() as db:
        meta = await db.get(MetaEvent, meta_event_id)
        if not meta:
            logger.warning("CAPI job for missing MetaEvent %d", meta_event_id)
            return
        if meta.status not in (MetaEventStatus.PENDING, MetaEventStatus.QUEUED, MetaEventStatus.RETRYING):
            # already sent or dead
            return

        # load pixel token
        pixel_token = None
        # try account tokens first, then meta_pixels table
        from shared.models import MetaPixel, TelegramAccount, Campaign
        # check via campaign -> account
        if meta.campaign_id:
            camp = await db.get(Campaign, meta.campaign_id)
            if camp and camp.account_id:
                acct = await db.get(TelegramAccount, camp.account_id)
                if acct and acct.meta_capi_token:
                    pixel_token = acct.meta_capi_token
        if not pixel_token:
            r = await db.execute(select(MetaPixel).where(MetaPixel.pixel_id == meta.pixel_id, MetaPixel.is_active == True).limit(1))
            mp = r.scalar_one_or_none()
            if mp and mp.access_token:
                pixel_token = mp.access_token

        # attempt
        meta.attempt_count = (meta.attempt_count or 0) + 1
        meta.status = MetaEventStatus.RETRYING if meta.attempt_count > 1 else MetaEventStatus.QUEUED
        await db.commit()

    result = await send_to_meta(meta, pixel_token)

    async with AsyncSessionLocal() as db:
        meta = await db.get(MetaEvent, meta_event_id)
        if not meta:
            return
        status_code = result.get("status", 0)
        data = result.get("data") or {}
        error = result.get("error")

        if error and not data:
            # network or missing credential
            meta.http_status = status_code or None
            meta.error_message = error[:2000] if error else ""
            meta.meta_response = json.dumps({"error": error})[:8000]
            if is_retryable(status_code or 0, error) and meta.attempt_count < MAX_ATTEMPTS:
                delay = backoff_seconds(meta.attempt_count)
                meta.status = MetaEventStatus.RETRYING
                meta.next_attempt_at = bind_ts(datetime.now(timezone.utc) + timedelta(seconds=delay))
                await db.commit()
                # re-enqueue delayed
                try:
                    import redis.asyncio as aioredis
                    redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
                    # use delayed zset via queue helper
                    from shared.queue import requeue_delayed
                    job = {"meta_event_id": meta.id, "event_id": meta.event_id}
                    await requeue_delayed(settings.QUEUE_META_CAPI, {"payload": job, "job_id": meta.event_id}, delay)
                    await redis.publish("tg_notifications", json.dumps({"target_user_id": meta.owner_user_id, "type": "meta_retry", "title": "CAPI retry scheduled", "body": f"{meta.event_name} attempt {meta.attempt_count}/{MAX_ATTEMPTS} in {delay}s"}))
                    await redis.aclose()
                except Exception:
                    pass
                log_event(logger, "META_EVENT_FAILED", meta_event_id=meta.id, status=status_code, attempt=meta.attempt_count, retry_in=delay, error=error)
            else:
                meta.status = MetaEventStatus.DEAD_LETTER if meta.attempt_count >= MAX_ATTEMPTS else MetaEventStatus.FAILED
                meta.next_attempt_at = None
                await db.commit()
                log_event(logger, "META_EVENT_DEAD_LETTER" if meta.status == MetaEventStatus.DEAD_LETTER else "META_EVENT_FAILED", meta_event_id=meta.id, status=status_code, attempt=meta.attempt_count, error=error)
                # also update conversion_log if linked
                if meta.conversion_log_id:
                    from shared.models import ConversionLog, ConversionStatus
                    clog = await db.get(ConversionLog, meta.conversion_log_id)
                    if clog:
                        clog.status = ConversionStatus.error
                        clog.error_detail = error[:2000]
                        clog.fbtrace_id = None
                        await db.commit()
                # notify
                try:
                    import redis.asyncio as aioredis
                    redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
                    await redis.publish("tg_notifications", json.dumps({"target_user_id": meta.owner_user_id, "type": "meta_failed", "title": "CAPI failed", "body": f"{meta.event_name} failed after {meta.attempt_count} attempts"}))
                    await redis.aclose()
                except Exception:
                    pass
            return

        # HTTP response from Meta
        if status_code == 200 and data.get("events_received", 0) > 0:
            meta.http_status = 200
            meta.meta_response = json.dumps(data)[:8000]
            meta.fbtrace_id = data.get("fbtrace_id")
            meta.status = MetaEventStatus.SENT
            meta.sent_at = bind_ts(datetime.now(timezone.utc))
            meta.error_message = None
            await db.commit()
            log_event(logger, "META_EVENT_SENT", meta_event_id=meta.id, event_name=meta.event_name, fbtrace_id=meta.fbtrace_id, attempt=meta.attempt_count)
            # update conversion log
            if meta.conversion_log_id:
                from shared.models import ConversionLog, ConversionStatus
                clog = await db.get(ConversionLog, meta.conversion_log_id)
                if clog:
                    clog.status = ConversionStatus.fired
                    clog.fbtrace_id = meta.fbtrace_id
                    clog.meta_event_id = meta.event_id
                    await db.commit()
            # notify dashboard
            try:
                import redis.asyncio as aioredis
                redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
                await redis.publish("tg_notifications", json.dumps({"target_user_id": meta.owner_user_id, "type": "conversion", "title": f"✅ {meta.event_name} sent", "body": f"Pixel {meta.pixel_id} — fbtrace {meta.fbtrace_id or ''}", "data": {"campaign_id": meta.campaign_id, "event_name": meta.event_name}}))
                await redis.aclose()
            except Exception:
                pass
        else:
            # 4xx or 5xx with JSON error
            err_detail = json.dumps(data)[:2000] if data else result.get("error", "unknown")
            meta.http_status = status_code
            meta.meta_response = json.dumps(data)[:8000] if data else None
            meta.fbtrace_id = data.get("fbtrace_id") if isinstance(data, dict) else None
            meta.error_message = err_detail
            if is_retryable(status_code, err_detail) and meta.attempt_count < MAX_ATTEMPTS:
                delay = backoff_seconds(meta.attempt_count)
                meta.status = MetaEventStatus.RETRYING
                meta.next_attempt_at = bind_ts(datetime.now(timezone.utc) + timedelta(seconds=delay))
                await db.commit()
                try:
                    from shared.queue import requeue_delayed
                    await requeue_delayed(settings.QUEUE_META_CAPI, {"payload": {"meta_event_id": meta.id}, "job_id": meta.event_id}, delay)
                except Exception:
                    pass
                log_event(logger, "META_EVENT_FAILED", meta_event_id=meta.id, status=status_code, attempt=meta.attempt_count, retry_in=delay, error=err_detail[:200])
            else:
                meta.status = MetaEventStatus.DEAD_LETTER if meta.attempt_count >= MAX_ATTEMPTS else MetaEventStatus.FAILED
                meta.next_attempt_at = None
                await db.commit()
                log_event(logger, "META_EVENT_DEAD_LETTER" if meta.status == MetaEventStatus.DEAD_LETTER else "META_EVENT_FAILED", meta_event_id=meta.id, status=status_code, attempt=meta.attempt_count, error=err_detail[:200])
                if status_code >= 400 and status_code < 500:
                    # notify admin about bad config
                    try:
                        import redis.asyncio as aioredis
                        redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
                        await redis.publish("tg_notifications", json.dumps({"target_user_id": meta.owner_user_id, "type": "meta_failed", "title": "CAPI config error", "body": f"Pixel {meta.pixel_id} {status_code}: {err_detail[:120]}"}))
                        await redis.aclose()
                    except Exception:
                        pass

async def heartbeat_loop():
    import redis.asyncio as aioredis
    redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    while True:
        try:
            await redis.setex("worker:heartbeat:meta_capi", settings.WORKER_HEARTBEAT_TTL, datetime.now(timezone.utc).isoformat())
        except Exception:
            pass
        await asyncio.sleep(30)

async def delayed_promoter():
    from shared.queue import promote_delayed
    while True:
        try:
            n = await promote_delayed(settings.QUEUE_META_CAPI)
            if n:
                logger.info("Promoted %d delayed CAPI jobs", n)
        except Exception as e:
            logger.warning("delayed promoter error: %s", e)
        await asyncio.sleep(5)

async def worker_loop():
    import redis.asyncio as aioredis
    # also handle jobs that were already in RETRYING with next_attempt_at due
    while True:
        # check for due RETRYING rows that may have missed delayed queue (e.g. after restart)
        try:
            async with AsyncSessionLocal() as db:
                due_r = await db.execute(select(MetaEvent).where(MetaEvent.status == MetaEventStatus.RETRYING, MetaEvent.next_attempt_at != None, MetaEvent.next_attempt_at <= bind_ts(datetime.now(timezone.utc))).limit(20))
                for me in due_r.scalars().all():
                    from shared.queue import enqueue
                    await enqueue(settings.QUEUE_META_CAPI, {"meta_event_id": me.id, "event_id": me.event_id})
                    me.status = MetaEventStatus.QUEUED
                    await db.commit()
        except Exception as e:
            logger.warning("retry due check failed: %s", e)

        # BRPOP with timeout to also allow heartbeat/promotion
        try:
            redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
            item = await redis.brpop(settings.QUEUE_META_CAPI, timeout=5)
            await redis.aclose()
            if not item:
                continue
            _, raw = item
            job = json.loads(raw)
            payload = job.get("payload") or job
            meta_id = payload.get("meta_event_id")
            if not meta_id:
                # legacy: job directly is meta_event_id
                meta_id = job.get("meta_event_id") or job.get("id")
            if not meta_id:
                logger.warning("CAPI job missing meta_event_id: %s", job)
                continue
            await process_one(int(meta_id))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("CAPI worker loop error: %s", e)
            await asyncio.sleep(2)

async def main():
    setup_logging("capi_worker")
    validate_or_exit("capi_worker")
    logger.info("=== CAPI Worker starting ===")
    await init_db()
    hb = asyncio.create_task(heartbeat_loop())
    promoter = asyncio.create_task(delayed_promoter())
    worker = asyncio.create_task(worker_loop())
    stop_event = asyncio.Event()
    def _shutdown():
        logger.info("CAPI worker shutdown")
        stop_event.set()
    loop = asyncio.get_running_loop()
    try:
        import signal
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown)
    except Exception:
        pass
    try:
        await asyncio.wait([worker, hb, promoter, asyncio.create_task(stop_event.wait())], return_when=asyncio.FIRST_COMPLETED)
    finally:
        hb.cancel(); promoter.cancel(); worker.cancel()
        try:
            await hb; await promoter; await worker
        except asyncio.CancelledError:
            pass
        logger.info("CAPI worker stopped")

if __name__ == "__main__":
    asyncio.run(main())
