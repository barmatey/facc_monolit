import json
from pathlib import Path

import pandas as pd


BASE_PATH = f"C:/Users/barma/PycharmProjects/facc_monolit/tests/sample_data"


def load_source_values() -> list[dict]:
    path = Path(f"{BASE_PATH}/source_table.csv")
    data = pd.read_csv(path)
    data['total_start_date'] = pd.to_datetime(data['total_start_date'])
    data['total_end_date'] = pd.to_datetime(data['total_end_date'])
    data['wcols'] = data['wcols'].apply(lambda x: json.loads(x))
    data = data.to_dict(orient='records')
    return data


def load_wire_values() -> list[dict]:
    path = Path(f"{BASE_PATH}/wire_table.csv")
    data = pd.read_csv(path)
    data['date'] = pd.to_datetime(data['date'])
    data = data.to_dict(orient='records')
    return data


def load_sheet_values() -> list[dict]:
    path = Path(f"{BASE_PATH}/sheet_table.csv")
    data = pd.read_csv(path)
    data = data.to_dict(orient='records')
    return data
