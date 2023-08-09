import datetime

import loguru
import numpy as np
import pandas as pd
import pytest
from pathlib import Path

import helpers
from src.finrep.wire import Wire
from src.finrep.group import BalanceGroup
from src.finrep.interval import Interval
from src.finrep.report import BalanceReport

SARMAT_PATH = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
SIMPLE_BALANCE_GROUP = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_group.csv")
SIMPLE_BALANCE_REPORT = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_report.json")


@pytest.fixture(scope='module')
def wire():
    df = pd.read_csv(SARMAT_PATH, encoding='utf-8')
    df['date'] = pd.to_datetime(df['date'])
    wire = Wire(df)
    return wire


@pytest.fixture(scope='module')
def simple_balance_group():
    df = pd.read_csv(SIMPLE_BALANCE_GROUP).apply(lambda col: pd.to_numeric(col, errors='ignore'))
    group = BalanceGroup(df, ccols=['sender'], fixed_ccols=['sender'])
    return group


@pytest.fixture(scope='module')
def interval():
    data = {
        "start_date": pd.Timestamp("2021-04-01T09:07:13.363Z"),
        "end_date": pd.Timestamp("2021-04-30T09:07:13.363Z"),
        "iyear": 0,
        "imonth": 1,
        "iday": 0,
    }
    interval = Interval(**data)
    return interval


def test_create_simple_balance(wire: Wire, simple_balance_group: BalanceGroup, interval: Interval):
    with open(SIMPLE_BALANCE_REPORT) as data:
        expected = (
            pd.read_json(data, encoding='utf8', orient='records')
            .set_index(['level_0', 'level 1'])
            .rename({"1619740800000": datetime.date(2021, 4, 30)}, axis=1)
        )


    report_df = (
        BalanceReport()
        .create_report_df(wire, simple_balance_group, interval)
        .sort_by_group(simple_balance_group)
        .drop_zero_rows()
        .calculate_saldo()
        .get_report_df()
        .round(2)
    )
    loguru.logger.success(f'\n\nEXPECTED:'
                          f'\n\n{expected}\n\n')

    loguru.logger.success(f'\n\nREAL:'
                          f'\n\n{report_df}')
    # pd.testing.assert_frame_equal(expected, report_df)
