from pathlib import Path

import pandas as pd
import pytest
import pytest_asyncio
from sqlalchemy import insert

from src.repository_postgres_new.source import SourceModel
from src.repository_postgres_new.wire import WireModel

from .conftest import client, override_get_async_session

test_source_id = 17


@pytest_asyncio.fixture(autouse=True, scope='module')
async def create_wires():
    # Create source
    source_data = {"id": test_source_id, "title": "test_source"}

    # Load wires
    path = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
    wires_data = pd.read_csv(path, encoding='utf-8')

    async with override_get_async_session() as session:
        await session.execute(insert(SourceModel), source_data)
        await session.execute(insert(WireModel), wires_data)
        await session.commit()


@pytest.mark.asyncio
def test_group_create_return_200():
    url = f"/group"


@pytest.mark.asyncio
def test_temp():
    assert 1 == 1


@pytest.mark.asyncio
def test_total_recalculate_return_200():
    group_id = 1
    url = f"/group/{group_id}/total-recalculate"
    response = client.patch(url)
    assert response.status_code == 200
