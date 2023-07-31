import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from sqlalchemy import insert

from src.repository_postgres_new.normalizer import Normalizer
from src.repository_postgres_new.sheet import RowModel, ColModel, CellModel, SheetModel
from .conftest import override_get_async_session, client


@pytest_asyncio.fixture(autouse=True, scope='module')
async def insert_fake_sheet():
    sheet_id = 13
    row_ids = list(range(1, 12))
    col_ids = list(range(1, 4))
    cell_ids = list(range(1, 34))

    columns = ["first_col", "second_col", "third_col", ]
    records = [
        (10, "Hello", False),
        (20, "World", True),
        (30, "Jimmy", True),
        (40, "Kelly", False),
        (50, "Jane", True),
        (60, "Romeo", True),
        (70, "Romeo", False),
        (80, "Hello", True),
        (90, "Kelly", True),
        (100, "Bobby", False),
    ]
    df = pd.DataFrame.from_records(records, columns=columns)

    normalizer = Normalizer(df, drop_index=True, drop_columns=False)
    normalizer.normalize()

    rows = normalizer.get_normalized_rows()
    rows['id'] = row_ids
    rows['sheet_id'] = sheet_id

    cols = normalizer.get_normalized_cols()
    cols['id'] = col_ids
    cols['sheet_id'] = sheet_id

    cells = normalizer.get_normalized_cells()
    cells['id'] = cell_ids
    cells['row_id'] = np.repeat(row_ids, len(col_ids))
    cells['col_id'] = col_ids * len(row_ids)
    cells['sheet_id'] = sheet_id

    async with override_get_async_session() as session:
        _ = await session.execute(insert(SheetModel).values({"id": sheet_id}))
        _ = await session.execute(insert(RowModel).values(rows.to_dict(orient='records')))
        _ = await session.execute(insert(ColModel).values(cols.to_dict(orient='records')))
        _ = await session.execute(insert(CellModel), cells.to_dict(orient='records'))
        await session.commit()


@pytest.mark.asyncio
async def test_update_cell_return_200():
    sheet_id = 13
    url = f"/sheet/{sheet_id}/update-cell"
    data = {
        "id": 6,
        "sheet_id": sheet_id,
        "value": "New value",
        "dtype": "TEXT",
    }
    response = client.patch(url, json=data)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_update_freeze_cell_return_423():
    sheet_id = 13
    url = f"/sheet/{sheet_id}/update-cell"
    data = {
        "id": 0,
        "sheet_id": sheet_id,
        "value": "New value",
        "dtype": "TEXT",
    }
    response = client.patch(url, json=data)
    assert response.status_code == 423


@pytest.mark.asyncio
async def test_partial_update_many_cells_return_200():
    sheet_id = 13
    url = f"/sheet/{sheet_id}/update-cell-bulk"
    data = [
        {
            "id": 0,
            "sheet_id": sheet_id,
            "value": "zero",
            "dtype": "TEXT",
        },
        {
            "id": 5,
            "sheet_id": sheet_id,
            "value": "first",
            "dtype": "TEXT",
        },
        {
            "id": 15,
            "sheet_id": sheet_id,
            "value": "second",
            "dtype": "TEXT",
        },
    ]
    response = client.patch(url, json=data)
    assert response.status_code == 200
