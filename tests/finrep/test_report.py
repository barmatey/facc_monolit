import datetime

import pandas as pd
import pytest
from pathlib import Path

from src.finrep.wire import Wire
from src.finrep.group import BalanceGroup, ProfitGroup
from src.finrep.interval import Interval
from src.finrep.report import BalanceReport, ProfitReport

from src.helpers import log

SARMAT_PATH = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")

SIMPLE_BALANCE_GROUP = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_group.csv")
SIMPLE_BALANCE_REPORT = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_report.json")

COMPLEX_BALANCE_GROUP = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/complex_balance_group.json")
COMPLEX_BALANCE_REPORT = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/complex_balance_report.json")

COMPLEX_PROFIT_GROUP = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/complex_profit_group.json")
COMPLEX_PROFIT_REPORT = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/complex_profit_report.json")


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
def complex_balance_group():
    with open(COMPLEX_BALANCE_GROUP) as data:
        group_df = pd.read_json(data, encoding='utf8', orient='records')
    group = BalanceGroup(group_df, ccols=['sender', 'subconto_first'], fixed_ccols=['sender', 'subconto_first'])
    return group


@pytest.fixture(scope='module')
def complex_profit_group():
    with open(COMPLEX_PROFIT_GROUP) as data:
        group_df = pd.read_json(data, encoding='utf8', orient='records')
    group = ProfitGroup(group_df, ccols=['sender', 'subconto_first'], fixed_ccols=['sender', 'subconto_first'])
    return group


@pytest.fixture(scope='module')
def interval():
    data = {
        "start_date": pd.Timestamp("2021-03-01T00:00:00.000Z"),
        "end_date": pd.Timestamp("2021-07-30T00:00:00.000Z"),
        "iyear": 0,
        "imonth": 1,
        "iday": 0,
    }
    interval = Interval(**data)
    return interval


def test_create_simple_balance(wire: Wire, simple_balance_group: BalanceGroup):
    with open(SIMPLE_BALANCE_REPORT) as data:
        expected = (
            pd.read_json(data, encoding='utf8', orient='records')
            .set_index(['level 0', 'level 1'])
            .rename({"1619740800000": datetime.date(2021, 4, 30)}, axis=1)
        )

    interval = {
        "start_date": pd.Timestamp("2021-04-01T00:00:00.000Z"),
        "end_date": pd.Timestamp("2021-04-30T00:00:00.000Z"),
        "iyear": 0,
        "imonth": 1,
        "iday": 0,
    }
    interval = Interval(**interval)

    report_df = (
        BalanceReport(wire, simple_balance_group, interval)
        .create_report_df()
        .sort_by_group()
        .drop_zero_rows()
        .calculate_saldo()
        .get_report_df()
    )
    pd.testing.assert_frame_equal(expected, report_df)


def test_create_complex_balance(wire: Wire, complex_balance_group: BalanceGroup, interval: Interval):
    with open(COMPLEX_BALANCE_REPORT) as json:
        expected = pd.read_json(json, encoding='utf8', orient='records')
        expected = expected.set_index(['level 0', 'level 1', 'level 2'])
        expected.columns = [pd.to_datetime(x).date() for x in expected.columns]

    report_df = (
        BalanceReport(wire, complex_balance_group, interval)
        .create_report_df()
        .sort_by_group()
        .drop_zero_rows()
        .calculate_saldo()
        .get_report_df()
        .round(2)
    )

    pd.testing.assert_frame_equal(expected, report_df)


def test_create_complex_profit(wire: Wire, complex_profit_group: ProfitGroup, interval: Interval):
    with open(COMPLEX_PROFIT_REPORT) as data:
        expected_report_df = pd.read_json(data).set_index(['level 1', 'level 2'])
        expected_report_df.columns = [pd.to_datetime(x).date() for x in expected_report_df.columns]

    real_report_df = (
        ProfitReport(wire, complex_profit_group, interval)
        .create_report_df()
        .sort_by_group()
        .calculate_total()
        .drop_zero_rows()
        .get_report_df()
    )
    pd.testing.assert_frame_equal(expected_report_df, real_report_df)
