from pathlib import Path

import loguru
import pandas as pd
import pytest
import pytest_asyncio
from sqlalchemy import insert

from src.repository_postgres_new.category import CategoryModel

from .conftest import client, override_get_async_session


@pytest_asyncio.fixture(autouse=True, scope='module')
async def create_standard_categories():
    async with override_get_async_session() as session:
        data = [
            {"value": "BALANCE", "id": 1},
            {"value": "PROFIT", "id": 2},
            {"value": "CASHFLOW", "id": 3},
        ]
        stmt = insert(CategoryModel).values(data)
        await session.execute(stmt)
        await session.commit()


@pytest_asyncio.fixture(autouse=True, scope='module')
async def create_source():
    # Create source
    url = "/source-db"
    source = client.post(url, json={"title": "test_group_source"}).json()
    source_id = source['id']

    # Append wires
    url = f"/source-db/{source_id}"
    path = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
    csv = pd.read_csv(path, encoding="utf8").to_csv(index=False)
    client.post(url, files={"file": csv})

    return source_id


@pytest.mark.asyncio
async def test_group_create_return_200(create_source):
    source_id = create_source
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)
    # assert response.status_code == 200


@pytest.mark.asyncio
async def test_temp():
    assert 1 == 1

# @pytest.mark.asyncio
# def test_total_recalculate_return_200():
#     group_id = 1
#     url = f"/group/{group_id}/total-recalculate"
#     response = client.patch(url)
#     assert response.status_code == 200
