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
async def source_id():
    # Create source
    url = "/source-db"
    source = client.post(url, json={"title": "test_source"}).json()
    id_ = source['id']

    # Append wires
    url = f"/source-db/{id_}"
    path = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
    csv = pd.read_csv(path, encoding="utf8").head(1000).to_csv(index=False)
    client.post(url, files={"file": csv})

    return id_


@pytest.mark.asyncio
async def test_group_create_return_200(source_id):
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_group_return_200(source_id):
    # Create group
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)

    # Get group
    url = f"/group/{response.json().get('id')}"
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_many_groups_return_200():
    url = f"/group"
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_partial_update_group_return_200(source_id):
    # Create group
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)

    # Partial update
    url = f"/group/{response.json().get('id')}"
    data = {
        "title": "New group title"
    }
    response = client.patch(url, json=data)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_delete_group_return_200(source_id):
    # Create group
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)

    # Delete
    url = f"/group/{response.json().get('id')}"
    response = client.delete(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_total_recalculate_return_200(source_id):
    # Create group
    url = f"/group"
    data = {
        "title": "test_group",
        "source_id": source_id,
        "category": "BALANCE",
        "columns": ["sender", ],
        "fixed_columns": ["sender"],
    }
    response = client.post(url, json=data)
    group_id = response.json().pop("id")

    # Recalculate group
    url = f"/group/{group_id}/total-recalculate"
    response = client.patch(url)
    assert response.status_code == 200
