from pathlib import Path

import pytest
import pandas as pd

from .conftest import client, BASE_FILE_PATH


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
    # Create source
    url = "/source-db"
    data = {"title": "BigBen"}
    created_id = client.post(url, json=data).json()['id']

    # Test endpoint
    url = f"/source-db/{created_id}"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json()['title'] == "BigBen"


@pytest.mark.asyncio
async def test_get_many_sources():
    url = "/source-db"
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_delete_one_source():
    # Create
    url = "/source-db"
    data = {"title": "Source for delete"}
    source = client.post(url, json=data).json()

    # Delete
    url = f"/source-db/{source['id']}"
    response = client.delete(url)
    assert response.status_code == 200
    assert response.json() == source['id']


@pytest.mark.asyncio
async def test_append_wires_from_csv():
    # Create source
    url = "/source-db"
    source = client.post(url, json={"title": "temp"}).json()

    # Append wires
    url = f"/source-db/{source['id']}"
    path = Path(f"{BASE_FILE_PATH}/tests/files/sarmat.csv")
    csv = pd.read_csv(path, encoding="utf8").head(10).to_csv(index=False)
    response = client.post(url, files={"file": csv})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_one_wire_with_correct_data():
    # Create source
    url = "/source-db"
    source = client.post(url, json={"title": "temp"}).json()

    # Create wire
    url = "/wire"
    data = {
        "source_id": source['id'],
        "date": "2023-07-30T07:11:05.771Z",
        "sender": 0,
        "receiver": 1,
        "debit": 100,
        "credit": 0,
        "sub1": None,
        "sub2": None,
        "comment": "hello!",
    }
    response = client.post(url, json=data)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_create_many_wires_with_correct_data():
    # Create source
    url = "/source-db"
    source = client.post(url, json={"title": "temp"}).json()

    url = "/wire/many"
    data = [
        {
            "source_id": source['id'],
            "date": "2023-07-30T07:11:05.771Z",
            "sender": 0,
            "receiver": x,
            "debit": x*13,
            "credit": 0,
            "sub1": None,
            "sub2": None,
            "comment": "hello!",
        }
        for x in range(1, 51)
    ]
    response = client.post(url, json=data)
    assert response.status_code == 200


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
        "sub1": None,
        "sub2": None,
        "comment": "hello!",
    }
    wire: dict = client.post(url, json=data).json()

    # Delete created
    url = f"/wire/{wire['id']}"
    response = client.delete(url)
    assert response.status_code == 200
    assert response.json() == wire['id']
