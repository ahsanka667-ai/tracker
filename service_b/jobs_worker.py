"""
service_b/jobs_worker.py — Background jobs: retention sweep, flow waits, health

- Periodic retention cleanup (clicks, events, meta_events beyond retention)
- Flow wait-node progression
- Heartbeat for health checks
"""
import asyncio, logging, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timedelta, timezone
from sqlalchemy import delete, select

from shared.config import get_settings, validate_or_exit
from shared.logging_config import setup_logging, log_event
from shared.database import AsyncSessionLocal, init_db

logger = logging.getLogger(__name__)
settings = get_settings()

async def retention_sweep():
    """Delete expired data beyond retention window."""
    days = settings.effective_click_retention_days
    if days <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    from shared.database import bind_ts
    cut = bind_ts(cutoff)
    try:
        async with AsyncSessionLocal() as db:
            from shared.models import Click, TelegramEvent, ConversionLog
            # clicks
            r = await db.execute(delete(Click).where(Click.created_at < cut))
            # we keep telegram_events & conversions for analytics longer, but respect DATA_RETENTION_DAYS
            data_cutoff = datetime.now(timezone.utc) - timedelta(days=settings.DATA_RETENTION_DAYS) if settings.DATA_RETENTION_DAYS else None
            if data_cutoff:
                dcut = bind_ts(data_cutoff)
                await db.execute(delete(TelegramEvent).where(TelegramEvent.created_at < dcut))
                # keep conversions but could also prune
            await db.commit()
            if r.rowcount:
                log_event(logger, "RETENTION_SWEEP", deleted_clicks=r.rowcount, cutoff=cutoff.isoformat())
    except Exception as e:
        logger.warning("Retention sweep failed: %s", e)

async def flow_wait_progression():
    """Resume flows that were waiting."""
    try:
        from shared.models import FlowRun
        from shared.database import bind_ts
        async with AsyncSessionLocal() as db:
            r = await db.execute(select(FlowRun).where(FlowRun.status == "waiting"))
            for run in r.scalars().all():
                # Check if wait expired (stored in context json)
                import json
                try:
                    ctx = json.loads(run.context) if run.context else {}
                    wait_until = ctx.get("wait_until")
                    if wait_until:
                        wt = datetime.fromisoformat(wait_until.replace("Z", "+00:00"))
                        if datetime.now(timezone.utc) >= wt:
                            run.status = "running"
                            # advance to next node (simplified)
                            run.context = json.dumps({**ctx, "wait_completed": True})
                except Exception:
                    continue
            await db.commit()
    except Exception as e:
        logger.warning("Flow wait progression failed: %s", e)

async def heartbeat_loop():
    import redis.asyncio as aioredis
    redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    while True:
        try:
            await redis.setex("worker:heartbeat:jobs", settings.WORKER_HEARTBEAT_TTL, datetime.now(timezone.utc).isoformat())
        except Exception:
            pass
        await asyncio.sleep(30)

async def main_loop():
    setup_logging("jobs_worker")
    validate_or_exit("jobs_worker")
    await init_db()
    logger.info("=== Jobs Worker starting ===")
    hb = asyncio.create_task(heartbeat_loop())
    # initial sweep after 60s, then periodic
    await asyncio.sleep(60)
    while True:
        try:
            await retention_sweep()
            await flow_wait_progression()
        except Exception as e:
            logger.exception("Jobs worker error: %s", e)
        await asyncio.sleep(settings.RETENTION_SWEEP_INTERVAL_SECONDS)

async def main():
    stop = asyncio.Event()
    def _shutdown():
        stop.set()
    loop = asyncio.get_running_loop()
    try:
        import signal
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown)
    except Exception:
        pass
    task = asyncio.create_task(main_loop())
    hb = asyncio.create_task(heartbeat_loop())
    try:
        await asyncio.wait([task, hb, asyncio.create_task(stop.wait())], return_when=asyncio.FIRST_COMPLETED)
    finally:
        task.cancel(); hb.cancel()
        try:
            await task; await hb
        except asyncio.CancelledError:
            pass
        logger.info("Jobs worker stopped")

if __name__ == "__main__":
    import asyncio as _asyncio
    _asyncio.run(main())
