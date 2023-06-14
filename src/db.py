from loguru import logger
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = f"postgresql+asyncpg://postgres:145190hfp@127.0.0.1:5432/monolyt_db"
Base: DeclarativeMeta = declarative_base()

engine = create_async_engine(DATABASE_URL)


def async_session_generator():
    return sessionmaker(engine, class_=AsyncSession)


@asynccontextmanager
async def get_session() -> AsyncSession:
    try:
        async_session = async_session_generator()
        async with async_session() as session:
            yield session
    except Exception as err:
        logger.error(err)
        await session.rollback()
        raise
    finally:
        await session.close()
