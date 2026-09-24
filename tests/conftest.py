import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import pytest_asyncio

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["SECRET_KEY"] = "test-secret-key-that-is-long-enough-for-testing-32chars+-extra-long-for-ci-12345"
os.environ["ADMIN_USERNAME"] = "admin"
os.environ["ADMIN_PASSWORD"] = "admin1234"
os.environ["BASE_URL"] = "http://localhost:8000"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"

from shared.config import reload_settings
from shared.database import reset_engine, init_db, AsyncSessionLocal

from fakeredis import FakeServer
import fakeredis.aioredis as fakeredis_aioredis
import redis.asyncio as redis_asyncio

server = FakeServer()
orig_from_url = redis_asyncio.from_url
def fake_from_url(url, *args, **kwargs):
    return fakeredis_aioredis.FakeRedis(server=server, decode_responses=True)
redis_asyncio.from_url = fake_from_url

@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_db():
    reload_settings()
    reset_engine()
    await init_db()

@pytest_asyncio.fixture
async def db_session():
    async with AsyncSessionLocal() as session:
        yield session

@pytest.fixture
def fake_redis():
    return fakeredis_aioredis.FakeRedis(server=server, decode_responses=True)

@pytest_asyncio.fixture
async def client(fake_redis):
    import service_a.main as main
    main.redis_client = fake_redis
    from httpx import AsyncClient, ASGITransport
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

@pytest_asyncio.fixture
async def admin_token(client):
    from shared.models import DashboardUser
    from shared.security import hash_password
    from sqlalchemy import select
    async with AsyncSessionLocal() as db:
        r = await db.execute(select(DashboardUser).where(DashboardUser.telegram_id == -1))
        if not r.scalar_one_or_none():
            db.add(DashboardUser(telegram_id=-1, first_name="admin", login_username="admin", password_hash=hash_password("admin1234"), is_admin=True, is_active=True))
            await db.commit()
    resp = await client.post("/api/auth/login", json={"username":"admin","password":"admin1234"})
    assert resp.status_code == 200, resp.text
    return resp.json()["session_token"]

@pytest.fixture
def auth_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}
