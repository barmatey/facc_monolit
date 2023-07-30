from pathlib import Path

import pytest
import pandas as pd
from loguru import logger

from .conftest import client


@pytest.mark.asyncio
async def test_create_source():
    url = "/source-db"
    data = {
        "title": "hello",
    }
    response = client.post(url, json=data)
    assert response.status_code == 200
    assert data['title'] == response.json()['title']


@pytest.mark.asyncio
async def test_get_one_source():
    url = "/source-db/1"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json()['title'] == "hello"


@pytest.mark.asyncio
async def test_get_many_sources():
    url = "/source-db"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json()[0]['title'] == "hello"


@pytest.mark.asyncio
async def test_append_wires_from_csv():
    url = "/source-db/1"
    path = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
    csv = pd.read_csv(path, encoding="utf8").to_csv(index=False)
    response = client.post(url, files={"file": csv})
    assert response.status_code == 200
