import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import AsyncClient

from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

from fastapi.testclient import TestClient

from main import app
from src.db import get_async_session
from src.repository_postgres_new.base import BaseModel

DATABASE_URL_TEST = {
    'database': "test_monolyt_db",
    'drivername': 'postgresql+asyncpg',
    'username': 'postgres',
    'password': '145190hfp',
    'host': '127.0.0.1',
    'port': 5432,
    'query': {},
}
DATABASE_URL_TEST = URL(**DATABASE_URL_TEST)

engine_test = create_async_engine(DATABASE_URL_TEST, poolclass=NullPool)
async_session_maker = sessionmaker(engine_test, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def override_get_async_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session


app.dependency_overrides[get_async_session] = override_get_async_session


@pytest_asyncio.fixture(autouse=True, scope='module')
async def prepare_database():
    async with engine_test.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)
    yield


# SETUP
@pytest.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


client = TestClient(app)


@pytest.fixture(scope="session")
async def ac() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
