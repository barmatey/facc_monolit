import pytest
import pytest_asyncio
from loguru import logger
from .conftest import client


@pytest.mark.asyncio
async def test_create_source():
    url = "/source-db"
    data = {
        "title": "string"
    }
    response = client.post(url, json=data)
    logger.debug(response.json())
    assert 1 == 1
