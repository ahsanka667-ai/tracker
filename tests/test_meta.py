import pytest, json
from sqlalchemy import select

async def ensure_campaign_meta(client, auth_headers):
    resp = await client.get("/api/campaigns", headers=auth_headers)
    if resp.json():
        return resp.json()[0]
    resp = await client.get("/api/accounts", headers=auth_headers)
    accts = resp.json()
    if not accts:
        await client.post("/api/accounts/bot", json={"bot_token":"123456:TESTTOKEN1234567890123456789012","label":"auto bot","meta_pixel_id":"1234567890","meta_capi_token":"test"}, headers=auth_headers)
        resp = await client.get("/api/accounts", headers=auth_headers)
        accts = resp.json()
    acct_id = accts[0]["id"]
    resp = await client.post("/api/campaigns", json={"name":"Meta Camp","account_id":acct_id,"target_telegram_username":"@autobot","event_type":"Lead"}, headers=auth_headers)
    assert resp.status_code == 201
    camp_id = resp.json()["id"]
    await client.post(f"/api/campaigns/{camp_id}/triggers", json={"trigger_type":"bot_start","event_name":"Lead"}, headers=auth_headers)
    resp = await client.get("/api/campaigns", headers=auth_headers)
    return resp.json()[0]

@pytest.mark.asyncio
async def test_meta_fbc_not_regenerated(client, auth_headers):
    from shared.tracking import build_fbc, normalize_fbc_or_build
    fbclid = "testfbclid123"
    fbc1 = build_fbc(fbclid, creation_ms=1234567890123)
    assert fbc1 == "fb.1.1234567890123.testfbclid123"
    stored = "fb.1.1111111111111.testfbclid123"
    result = normalize_fbc_or_build(stored, fbclid)
    assert result == stored
    result2 = normalize_fbc_or_build(None, fbclid)
    assert result2 is not None
    assert result2.endswith(fbclid)

@pytest.mark.asyncio
async def test_meta_event_queue(client, auth_headers):
    camp = await ensure_campaign_meta(client, auth_headers)
    camp_id = camp["id"]
    resp = await client.post("/api/meta-pixels", json={"pixel_id":"987654321098765","access_token":"EAAtesttoken12345","test_event_code":"TEST123"}, headers=auth_headers)
    # may already exist
    assert resp.status_code in (200,201,409)
    resp = await client.post(f"/api/campaigns/{camp_id}/triggers", json={"trigger_type":"custom_event","event_name":"Purchase","value":99.99,"currency":"USD"}, headers=auth_headers)
    assert resp.status_code in (200,201)
    resp = await client.post("/api/events/custom", json={"telegram_user_id":55555,"event_name":"Purchase","campaign_id":camp_id,"value":99.99}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    from shared.database import AsyncSessionLocal
    from shared.models import MetaEvent
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(MetaEvent).where(MetaEvent.owner_user_id==-1, MetaEvent.event_name=="Purchase").order_by(MetaEvent.created_at.desc()).limit(1))
        me = r.scalar_one_or_none()
        assert me is not None
        assert me.event_name == "Purchase"
        assert me.status in ("QUEUED","PENDING","SENT","RETRYING")
        assert me.dedup_key is not None
        assert me.event_id is not None

@pytest.mark.asyncio
async def test_dedup_same_event_id(client, auth_headers):
    from shared.meta import dedup_key
    pid = "1234567890"
    eid = "uuid-1234"
    k1 = dedup_key("Lead", eid, pid)
    k2 = dedup_key("Lead", eid, pid)
    assert k1 == k2
    k3 = dedup_key("Purchase", eid, pid)
    assert k1 != k3

@pytest.mark.asyncio
async def test_fbp_never_invented(client, auth_headers):
    from shared.tracking import normalize_fbp
    assert normalize_fbp(None) is None
    assert normalize_fbp("") is None
    assert normalize_fbp("invalid") is None
    assert normalize_fbp("fb.1.1234567890.123456789") == "fb.1.1234567890.123456789"
    assert normalize_fbp(None, "fb.1.1111111111.2222222222") == "fb.1.1111111111.2222222222"
