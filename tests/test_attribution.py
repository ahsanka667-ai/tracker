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
async def test_attribution_last_touch(client, auth_headers):
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from shared.attribution import resolve_attribution, link_click_to_identity
    camp_data = await ensure_campaign(client, auth_headers)
    camp_id = camp_data["id"]
    slug = camp_data["slug"]
    resp = await client.get(f"/t/{slug}?fbclid=first_click_12345&sub1=first", follow_redirects=False)
    assert resp.status_code == 302
    resp = await client.get(f"/t/{slug}?fbclid=second_click_45678&sub1=second", follow_redirects=False)
    assert resp.status_code == 302
    from shared.models import DashboardUser
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(DashboardUser).where(DashboardUser.login_username == "admin"))
        owner = r.scalar_one()
        owner_id = owner.telegram_id
        r2 = await db.execute(select(Click).where(Click.campaign_id == camp_id).order_by(Click.created_at.asc()))
        clicks = r2.scalars().all()
        assert len(clicks) >= 2
        first, second = clicks[-2], clicks[-1]
        tg_id = 99999
        await link_click_to_identity(owner_id, tg_id, first, source="bot_start", sender=None)
        await link_click_to_identity(owner_id, tg_id, second, source="bot_start", sender=None)
        attr = await resolve_attribution(owner_id, tg_id)
        assert attr is not None
        assert attr["click_id"] == second.id
        assert attr["fbclid"] == "second_click_45678"

@pytest.mark.asyncio
async def test_channel_join_attribution_recovery(client, auth_headers):
    from shared.database import AsyncSessionLocal
    from shared.models import Click
    from shared.attribution import resolve_attribution, link_click_to_identity
    camp = await ensure_campaign(client, auth_headers)
    camp_id = camp["id"]
    slug = camp["slug"]
    resp = await client.get(f"/t/{slug}?fbclid=recover_test_78901", follow_redirects=False)
    assert resp.status_code == 302
    from shared.models import DashboardUser
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(DashboardUser).where(DashboardUser.login_username=="admin"))
        owner_id = r.scalar_one().telegram_id
        r2 = await db.execute(select(Click).where(Click.campaign_id==camp_id).order_by(Click.created_at.desc()).limit(1))
        click = r2.scalar_one()
        tg_id = 88888
        await link_click_to_identity(owner_id, tg_id, click, source="bot_start")
        attr = await resolve_attribution(owner_id, tg_id)
        assert attr is not None
        assert attr["fbclid"] == "recover_test_78901"
        assert attr["click_id"] == click.id

@pytest.mark.asyncio
async def test_identity_dedup(client, auth_headers):
    from shared.attribution import get_or_create_identity
    from shared.database import AsyncSessionLocal
    from sqlalchemy import select, func
    from shared.models import TelegramIdentity
    async with AsyncSessionLocal() as db:
        from shared.models import DashboardUser
        r = await db.execute(select(DashboardUser).where(DashboardUser.login_username=="admin"))
        owner_id = r.scalar_one().telegram_id
    id1 = await get_or_create_identity(owner_id, 77777, username="testuser", first_name="Test")
    id2 = await get_or_create_identity(owner_id, 77777, username="testuser2", first_name="Test2")
    assert id1.id == id2.id
    async with AsyncSessionLocal() as db:
        cnt = (await db.execute(select(func.count()).select_from(TelegramIdentity).where(TelegramIdentity.owner_user_id==owner_id, TelegramIdentity.telegram_user_id==77777))).scalar()
        assert cnt == 1
