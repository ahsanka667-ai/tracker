import pytest
import pytest_asyncio
from sqlalchemy import select

@pytest.mark.asyncio
async def test_click_capture_and_fbc(client, auth_headers):
    # create account
    resp = await client.post("/api/accounts/bot", json={"bot_token":"123456:TESTTOKEN123456789012345678", "label":"test bot", "meta_pixel_id":"1234567890", "meta_capi_token":"test_capi_token"}, headers=auth_headers)
    assert resp.status_code in (200,201), resp.text
    # list accounts to get id
    resp = await client.get("/api/accounts", headers=auth_headers)
    assert resp.status_code == 200
    acct_id = resp.json()[0]["id"]
    # create campaign
    resp = await client.post("/api/campaigns", json={"name":"Test Campaign","account_id":acct_id,"target_telegram_username":"@testbot","event_type":"Lead"}, headers=auth_headers)
    assert resp.status_code == 201, resp.text
    slug = resp.json()["slug"]
    # add trigger for bot_start
    camp_id = resp.json()["id"]
    resp = await client.post(f"/api/campaigns/{camp_id}/triggers", json={"trigger_type":"bot_start","event_name":"Lead"}, headers=auth_headers)
    assert resp.status_code == 201
    # capture click with fbclid and subs
    resp = await client.get(f"/t/{slug}?fbclid=IwAR_test12345&sub1=val1&sub2=val2&adset=adsetA&ad=ad1", follow_redirects=False)
    assert resp.status_code == 302
    loc = resp.headers["location"]
    assert "t.me/testbot?start=" in loc
    token = loc.split("start=")[-1]
    assert len(token) >= 6
    # verify click in DB
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(Click).where(Click.campaign_id == camp_id))
        clicks = r.scalars().all()
        assert len(clicks) == 1
        c = clicks[0]
        assert c.fbclid == "IwAR_test12345"
        assert c.fbc is not None
        assert c.fbc.startswith("fb.1.")
        assert c.fbc.endswith("IwAR_test12345")
        assert c.sub1 == "val1"
        assert c.sub2 == "val2"
        assert c.adset == "adsetA"
        assert c.ad == "ad1"
        # fbp should be None since not provided
        assert c.fbp is None
        # event_id present for dedup
        assert c.event_id is not None

@pytest.mark.asyncio
async def test_fbp_capture(client, auth_headers):
    resp = await client.get("/api/campaigns", headers=auth_headers)
    camp = resp.json()[0]
    slug = camp["slug"]
    # capture with _fbp cookie
    resp = await client.get(f"/t/{slug}?fbclid=abcd12345longfbclid123&fbp=fb.1.1234567890.987654321", follow_redirects=False)
    assert resp.status_code == 302
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from sqlalchemy import select, desc
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(Click).where(Click.campaign_id == camp["id"]).order_by(desc(Click.created_at)).limit(1))
        c = r.scalar_one()
        assert c.fbp == "fb.1.1234567890.987654321"
        assert c.fbc is not None

@pytest.mark.asyncio
async def test_crawler_not_counted(client, auth_headers):
    resp = await client.get("/api/campaigns", headers=auth_headers)
    slug = resp.json()[0]["slug"]
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from sqlalchemy import select, func
    async with AsyncSessionLocal() as db:
        before = (await db.execute(select(func.count()).select_from(Click))).scalar()
    resp = await client.get(f"/t/{slug}?fbclid=test", headers={"User-Agent":"TelegramBot (like TwitterBot)"}, follow_redirects=False)
    assert resp.status_code == 302
    async with AsyncSessionLocal() as db:
        after = (await db.execute(select(func.count()).select_from(Click))).scalar()
    assert after == before, "crawler should not create click"

@pytest.mark.asyncio
async def test_tracking_link_flow(client, auth_headers):
    # create domain
    resp = await client.post("/api/tracking-domains", json={"domain":"track.example.com"}, headers=auth_headers)
    # may already exist from previous run
    if resp.status_code == 409:
        resp2 = await client.get("/api/tracking-domains", headers=auth_headers)
        domain_id = resp2.json()[0]["id"]
    else:
        assert resp.status_code in (200,201), resp.text
        domain_id = resp.json()["id"]
    # create link
    resp = await client.get("/api/campaigns", headers=auth_headers)
    campaigns = resp.json()
    if not campaigns:
        # create campaign
        r = await client.get("/api/accounts", headers=auth_headers)
        acct_id = r.json()[0]["id"] if r.json() else None
        if not acct_id:
            await client.post("/api/accounts/bot", json={"bot_token":"123456:TESTTOKEN7890123456789012345678","label":"tmp"}, headers=auth_headers)
            r = await client.get("/api/accounts", headers=auth_headers)
            acct_id = r.json()[0]["id"]
        resp = await client.post("/api/campaigns", json={"name":"Tmp Camp","account_id":acct_id,"target_telegram_username":"@testbot","event_type":"Lead"}, headers=auth_headers)
        assert resp.status_code == 201
        camp_id = resp.json()["id"]
    else:
        camp_id = campaigns[0]["id"]
    resp = await client.post("/api/tracking-links", json={"destination":"@testbot2","campaign_id":camp_id,"domain_id":domain_id,"label":"mylink"}, headers=auth_headers)
    assert resp.status_code == 201
    slug = resp.json()["slug"]
    # click via /c/
    resp = await client.get(f"/c/{slug}?fbclid=link123&sub1=foo", follow_redirects=False)
    assert resp.status_code == 302
    assert "t.me/testbot2" in resp.headers["location"]

