"""
service_b/meta_capi.py — Meta Conversions API (CAPI) dispatcher v4

Full implementation:
- All Meta CAPI user_data fields (EMQ-maximizing)
- custom_data: value, currency, content_name, content_ids, order_id
- Event deduplication ID (event_id UUID)
- Retry with exponential backoff (3 attempts, 1.5s/3s/6s)
- Separate 4xx (don't retry) vs 5xx/timeout (retry) handling
"""
import asyncio, hashlib, json, time, uuid, logging
from typing import Any
import aiohttp

logger = logging.getLogger(__name__)

META_CAPI_ENDPOINT = "https://graph.facebook.com/v19.0/{pixel_id}/events"
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.5


def _sha256(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()


def _sha256_phone(phone: str) -> str:
    """Strip non-digits then SHA-256."""
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit())
    return hashlib.sha256(digits.encode()).hexdigest() if digits else ""


def build_user_data(
    telegram_id:  int | None = None,
    first_name:   str | None = None,
    last_name:    str | None = None,
    username:     str | None = None,
    phone:        str | None = None,
    client_ip:    str | None = None,
    user_agent:   str | None = None,
    fbclid:       str | None = None,
    fbp:          str | None = None,   # browser pixel cookie (_fbp)
) -> dict[str, Any]:
    """
    Build the fully-populated user_data payload for Meta CAPI.
    Every PII field is SHA-256 hashed per Meta's requirements.
    client_ip and user_agent are sent plain (Meta requirement).

    The more fields you provide, the higher the Event Match Quality (EMQ)
    score — directly impacting how well Meta can attribute conversions
    back to your ads.
    """
    ud: dict[str, Any] = {}

    # External identifier — SHA-256 of Telegram ID
    # This is the strongest signal we have since it's consistent
    # across all events for the same user.
    if telegram_id:
        ud["external_id"] = _sha256(str(telegram_id))

    # Name — partial names still help matching
    if first_name:
        ud["fn"] = _sha256(first_name)
    if last_name:
        ud["ln"] = _sha256(last_name)

    # Phone — extremely high EMQ if available (rare from Telegram)
    if phone:
        ud["ph"] = _sha256_phone(phone)

    # Browser / network signals — sent PLAIN (Meta hashes these server-side)
    if client_ip:
        ud["client_ip_address"] = client_ip
    if user_agent:
        ud["client_user_agent"] = user_agent

    # Facebook Click ID — if present, enables browser-level attribution
    if fbclid:
        # Meta's fbc format: fb.{version}.{creation_time}.{fbclid}
        ud["fbc"] = f"fb.1.{int(time.time() * 1000)}.{fbclid}"

    # Facebook browser pixel cookie — if passed through from landing page
    if fbp:
        ud["fbp"] = fbp

    return ud


async def fire_event(
    pixel_id:          str,
    capi_token:        str,
    event_name:        str,
    user_data:         dict[str, Any],
    event_source_url:  str = "https://t.me",
    # custom_data fields — all optional
    value:             float | None = None,
    currency:          str | None = None,
    content_name:      str | None = None,
    content_ids:       list[str] | None = None,
    order_id:          str | None = None,
    num_items:         int | None = None,
    extra_custom_data: dict[str, Any] | None = None,
    # Meta test events tool
    test_event_code:   str | None = None,
) -> dict[str, Any]:
    """
    Fire a single server-side conversion event to Meta CAPI.

    Returns Meta's response dict on success:
      { events_received: 1, fbtrace_id: "...", event_id: "..." }

    Returns { error: ..., status: ... } on failure.

    Retries transient errors (5xx, timeout, connection) up to 3 times.
    Never retries 4xx (bad token / bad pixel / malformed payload).
    """
    if not pixel_id or not capi_token:
        logger.warning("Skipping CAPI fire: missing pixel_id or capi_token")
        return {"skipped": True, "reason": "missing credentials"}

    event_id = str(uuid.uuid4())

    custom_data: dict[str, Any] = {}
    if value is not None:
        custom_data["value"] = value
    if currency:
        custom_data["currency"] = currency.upper()
    if content_name:
        custom_data["content_name"] = content_name
    if content_ids:
        custom_data["content_ids"] = content_ids
        custom_data["content_type"] = "product"
    if order_id:
        custom_data["order_id"] = order_id
    if num_items is not None:
        custom_data["num_items"] = num_items
    if extra_custom_data:
        custom_data.update(extra_custom_data)

    event_payload: dict[str, Any] = {
        "event_name":       event_name,
        "event_time":       int(time.time()),
        "event_id":         event_id,       # dedup with browser pixel
        "event_source_url": event_source_url,
        "action_source":    "website",
        "user_data":        user_data,
    }
    if custom_data:
        event_payload["custom_data"] = custom_data

    body: dict[str, Any] = {
        "data":         [event_payload],
        "access_token": capi_token,
    }
    if test_event_code:
        body["test_event_code"] = test_event_code

    url = META_CAPI_ENDPOINT.format(pixel_id=pixel_id)
    last_error: dict[str, Any] = {}

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
                async with session.post(url, json=body, headers={"Content-Type": "application/json"}) as resp:
                    data = await resp.json()

                    if resp.status == 200:
                        logger.info(
                            "CAPI ✅ [pixel=%s event=%s received=%d fbtrace=%s attempt=%d]",
                            pixel_id, event_name,
                            data.get("events_received", 0),
                            data.get("fbtrace_id", "N/A"), attempt,
                        )
                        data["event_id"] = event_id
                        return data

                    # 4xx = our fault (bad token, bad pixel, bad payload) — don't retry
                    if 400 <= resp.status < 500:
                        logger.error("CAPI ❌ 4xx [pixel=%s event=%s status=%d]: %s",
                                     pixel_id, event_name, resp.status, json.dumps(data))
                        return {"error": data, "status": resp.status, "event_id": event_id}

                    # 5xx = Meta's problem — worth retrying
                    logger.warning("CAPI ⚠️ 5xx [pixel=%s event=%s status=%d attempt=%d/%d]",
                                   pixel_id, event_name, resp.status, attempt, _MAX_RETRIES)
                    last_error = {"error": data, "status": resp.status, "event_id": event_id}

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as exc:
            logger.warning("CAPI network error [pixel=%s attempt=%d/%d]: %s",
                           pixel_id, attempt, _MAX_RETRIES, exc)
            last_error = {"error": str(exc), "network_failure": True, "event_id": event_id}
        except Exception as exc:
            logger.exception("CAPI unexpected error [pixel=%s]: %s", pixel_id, exc)
            return {"error": str(exc), "event_id": event_id}

        if attempt < _MAX_RETRIES:
            await asyncio.sleep(_BACKOFF_BASE * (2 ** (attempt - 1)))

    logger.error("CAPI gave up after %d attempts [pixel=%s event=%s]", _MAX_RETRIES, pixel_id, event_name)
    return last_error or {"error": "unknown failure", "event_id": event_id}


# ── Convenience wrappers ──────────────────────────────────────────────

async def fire_lead(pixel_id, capi_token, telegram_id, first_name=None,
                     client_ip="", user_agent="", fbclid="",
                     event_name="Lead", phone=None, username=None, **kwargs):
    ud = build_user_data(telegram_id=telegram_id, first_name=first_name,
                          phone=phone, username=username,
                          client_ip=client_ip, user_agent=user_agent, fbclid=fbclid)
    return await fire_event(pixel_id=pixel_id, capi_token=capi_token,
                             event_name=event_name, user_data=ud, **kwargs)


# Keep old name working (called from service_a test-pixel endpoint)
async def fire_lead_event(pixel_id, capi_token, telegram_id, first_name=None,
                           client_ip="", user_agent="", fbclid="", event_name="Lead"):
    return await fire_lead(pixel_id=pixel_id, capi_token=capi_token,
                            telegram_id=telegram_id, first_name=first_name,
                            client_ip=client_ip, user_agent=user_agent,
                            fbclid=fbclid, event_name=event_name)


async def fire_purchase(pixel_id, capi_token, telegram_id, value, currency="USD",
                         content_name=None, order_id=None, first_name=None,
                         phone=None, client_ip="", user_agent="", fbclid=""):
    ud = build_user_data(telegram_id=telegram_id, first_name=first_name,
                          phone=phone, client_ip=client_ip,
                          user_agent=user_agent, fbclid=fbclid)
    return await fire_event(pixel_id=pixel_id, capi_token=capi_token,
                             event_name="Purchase", user_data=ud,
                             value=value, currency=currency,
                             content_name=content_name, order_id=order_id)


# Keep old alias for service_a test-pixel endpoint
async def fire_conversion_event(pixel_id, capi_token, event_name, user_data,
                                 test_event_code=None, **kwargs):
    return await fire_event(pixel_id=pixel_id, capi_token=capi_token,
                             event_name=event_name, user_data=user_data,
                             test_event_code=test_event_code, **kwargs)


def _build_user_data(*args, **kwargs):
    """Alias for backward compatibility with service_a test-pixel endpoint."""
    return build_user_data(*args, **kwargs)
