import pytest
from sqlalchemy import select

async def ensure_campaign(client, auth_headers):
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
    resp = await client.post("/api/campaigns", json={"name":"Auto Campaign","account_id":acct_id,"target_telegram_username":"@autobot","event_type":"Lead"}, headers=auth_headers)
    assert resp.status_code == 201
    camp_id = resp.json()["id"]
    await client.post(f"/api/campaigns/{camp_id}/triggers", json={"trigger_type":"bot_start","event_name":"Lead"}, headers=auth_headers)
    resp = await client.get("/api/campaigns", headers=auth_headers)
    return resp.json()[0]

@pytest.mark.asyncio
async def test_crm_journey(client, auth_headers):
    camp = await ensure_campaign(client, auth_headers)
    slug = camp["slug"]
    resp = await client.get(f"/t/{slug}?fbclid=journey_test_12345", follow_redirects=False)
    assert resp.status_code == 302
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from shared.attribution import link_click_to_identity
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(Click).where(Click.campaign_id==camp["id"]).order_by(Click.created_at.desc()).limit(1))
        click = r.scalar_one()
        from shared.models import DashboardUser
        r2 = await db.execute(select(DashboardUser).where(DashboardUser.login_username=="admin"))
        owner_id = r2.scalar_one().telegram_id
        ident, _ = await link_click_to_identity(owner_id, 66666, click, source="bot_start")
        ident_id = ident.id
    resp = await client.post("/api/tags", json={"name":"VIP","color":"#ff0000"}, headers=auth_headers)
    if resp.status_code == 409:
        resp = await client.get("/api/tags", headers=auth_headers)
        tag_id = [t for t in resp.json() if t["name"]=="VIP"][0]["id"]
    else:
        assert resp.status_code in (200,201)
        tag_id = resp.json()["id"]
    resp = await client.post(f"/api/identities/{ident_id}/tags", json={"tag_id":tag_id}, headers=auth_headers)
    assert resp.status_code == 200
    resp = await client.post("/api/events/custom", json={"telegram_user_id":66666,"event_name":"Lead","campaign_id":camp["id"]}, headers=auth_headers)
    assert resp.status_code == 200
    resp = await client.get(f"/api/identities/{ident_id}/journey", headers=auth_headers)
    assert resp.status_code == 200
    journey = resp.json()["journey"]
    assert len(journey) >= 2
    types = [j["type"] for j in journey]
    assert "CLICK" in types
    resp = await client.get("/api/events", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1

@pytest.mark.asyncio
async def test_funnel_creation_and_stats(client, auth_headers):
    camp = await ensure_campaign(client, auth_headers)
    camp_id = camp["id"]
    await client.post(f"/api/campaigns/{camp_id}/triggers", json={"trigger_type":"keyword","event_name":"Purchase","keywords":"paid"}, headers=auth_headers)
    resp = await client.get(f"/api/campaigns/{camp_id}/triggers", headers=auth_headers)
    triggers = resp.json()
    assert len(triggers) >= 2
    trigger_ids = [t["id"] for t in triggers[:2]]
    resp = await client.post("/api/funnels", json={"name":"Test Funnel","campaign_id":camp_id,"trigger_ids":trigger_ids}, headers=auth_headers)
    assert resp.status_code in (200,201), resp.text
    funnel_id = resp.json().get("id") or resp.json().get("funnel_id")
    resp = await client.get(f"/api/funnels/{funnel_id}/stats", headers=auth_headers)
    assert resp.status_code == 200
    stats = resp.json()
    assert "steps" in stats
    assert len(stats["steps"]) == 2

@pytest.mark.asyncio
async def test_end_to_end_chain(client, auth_headers):
    camp = await ensure_campaign(client, auth_headers)
    camp_id = camp["id"]
    slug = camp["slug"]
    resp = await client.get(f"/t/{slug}?fbclid=e2e_test_99901&campaign=E2ECampaign&adset=TestAdset&ad=TestAd", follow_redirects=False)
    assert resp.status_code == 302
    await client.post("/api/meta-pixels", json={"pixel_id":"111111111111111","access_token":"EAAtest"}, headers=auth_headers)
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from shared.attribution import link_click_to_identity
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(Click).where(Click.campaign_id==camp_id).order_by(Click.created_at.desc()).limit(1))
        click = r.scalar_one()
        from shared.models import DashboardUser
        r2 = await db.execute(select(DashboardUser).where(DashboardUser.login_username=="admin"))
        owner_id = r2.scalar_one().telegram_id
        tg_id = 123456789
        ident, ev = await link_click_to_identity(owner_id, tg_id, click, source="bot_start")
        resp2 = await client.post("/api/events/custom", json={"telegram_user_id":tg_id,"event_name":"Lead","campaign_id":camp_id}, headers=auth_headers)
        assert resp2.status_code == 200
    resp = await client.get("/api/analytics/overview", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["clicks"] >= 1
    resp = await client.get("/api/events", headers=auth_headers)
    assert any(e["telegram_user_id"]==tg_id for e in resp.json())
    resp = await client.get("/api/meta-events", headers=auth_headers)
    assert resp.status_code == 200
    assert len(resp.json()) >= 1
