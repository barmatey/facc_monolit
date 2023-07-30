import math
import random
from pathlib import Path

import pytest
import pandas as pd

from src.wire import schema
from src.wire import entities

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


# @pytest.mark.asyncio
# async def test_append_wires_from_csv():
#     url = "/source-db/1"
#     path = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
#     csv = pd.read_csv(path, encoding="utf8").to_csv(index=False)
#     response = client.post(url, files={"file": csv})
#     assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_one_wire_with_correct_data():
    url = "/wire"
    data = {
        "source_id": 1,
        "date": "2023-07-30T07:11:05.771Z",
        "sender": 0,
        "receiver": 1,
        "debit": 100,
        "credit": 0,
        "subconto_first": None,
        "subconto_second": None,
        "comment": "hello!",
    }
    response = client.post(url, json=data)
    assert response.status_code == 200


# @pytest.mark.asyncio
# async def test_create_many_wires_with_correct_data():
#     url = "/wire"
#     data = [
#         {
#             "source_id": 1,
#             "date": "2023-07-30T07:11:05.771Z",
#             "sender": 0,
#             "receiver": x,
#             "debit": x*13,
#             "credit": 0,
#             "subconto_first": None,
#             "subconto_second": None,
#             "comment": "hello!",
#         }
#         for x in range(1, 51)
#     ]
#     response = client.post(url, json=data)
#     assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_many_wires_with_pagination():
    url = "/wire"
    params = {"source_id": 1, "paginate_from": 0, "paginate_to": 100, }
    response = client.get(url, params=params)
    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_one_wire_with_correct_filter():
    url = "/wire/1"
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_partial_update_one_wire():
    url = "/wire/1"
    before_update = client.get(url).json()

    after_update = before_update.copy()
    after_update['debit'] = 111
    after_update['comment'] = "Don't cry and give up. Cry. And keep go!"

    data = {'debit': after_update['debit'], 'comment': after_update['comment']}
    response = client.patch(url, json=data)
    assert response.status_code == 200
    assert response.json() == after_update


@pytest.mark.asyncio
async def test_delete_one_wire():
    # Create new wire
    url = "/wire"
    data = {
        "source_id": 1,
        "date": "2023-07-30T07:11:05.771Z",
        "sender": 0,
        "receiver": 1,
        "debit": 100,
        "credit": 0,
        "subconto_first": None,
        "subconto_second": None,
        "comment": "hello!",
    }
    wire: dict = client.post(url, json=data).json()

    # Delete created
    url = f"/wire/{wire['id']}"
    response = client.delete(url)
    assert response.status_code == 200
    assert response.json() == wire['id']
