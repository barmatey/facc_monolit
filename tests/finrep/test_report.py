import datetime

import loguru
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
COMPLEX_BALANCE_GROUP = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/complex_balance_group.json")
SIMPLE_BALANCE_REPORT = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_report.json")

log = loguru.logger.debug


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


def complex_balance_group_saved():
    df = pd.read_csv(SARMAT_PATH, encoding='utf-8')
    df['date'] = pd.to_datetime(df['date'])
    wire = Wire(df)
    group_df = BalanceGroup().create_group_df(wire, ccols=['sender', 'subconto_first']).get_group_df()
    group_df = group_df.loc[group_df['sender'] < 89]

    mapper = {
        0.0: "Zero",
        1.0: "Capital Assets",
        2.0: "Amortization",
        10.1: "Raw Materials",
        10.3: "Raw Materials",
        10.5: "Raw Materials",
        10.6: "Raw Materials",
        10.9: "Raw Materials",
        20.1: "Expenses",
        26.0: "Expenses",
        50.0: "Money",
        51.0: "Money",
        57.0: "Money",
        60.0: "Receivable",
        62.1: "Receivable",
        66.1: "Receivable",
        66.2: "Receivable",
        70.0: "Receivable",
        71.0: "Receivable",
        75.0: "Receivable",
        76.0: "Receivable",
        80.0: "Eighteen",
        81.0: "Eighteen",
        82.0: "Eighteen",
    }
    group_df['assets, level 1'] = group_df['assets, level 1'].replace(mapper)

    mapper = {
        0.0: "Zero",
        1.0: "No opinion",
        2.0: "Amortization",
        10.1: "Payable",
        10.3: "Payable",
        10.5: "Payable",
        10.6: "Payable",
        10.9: "Payable",
        20.1: "Payable",
        26.0: "Payable",
        50.0: "Payable",
        51.0: "Payable",
        57.0: "Payable",
        60.0: "Payable",
        62.1: "Payable",
        66.1: "Payable",
        66.2: "Payable",
        70.0: "Payable",
        71.0: "Payable",
        75.0: "Payable",
        76.0: "Payable",
        80.0: "Eighteen",
        81.0: "Eighteen",
        82.0: "Eighteen",
    }
    group_df['liabs, level 1'] = group_df['liabs, level 1'].replace(mapper)

    random_items = ["first_item", "second_item", "third_item"] * 77
    group_df['assets, level 2'] = random_items
    group_df['liabs, level 2'] = random_items

    # log(f'\n{group_df.to_string()}')
    # group_df.to_json(COMPLEX_BALANCE_GROUP, orient='records')


@pytest.fixture(scope='module')
def complex_balance_group():
    with open(COMPLEX_BALANCE_GROUP) as data:
        group_df = pd.read_json(data, encoding='utf8', orient='records')
    group = BalanceGroup(group_df, ccols=['sender', 'subconto_first'], fixed_ccols=['sender', 'subconto_first'])
    return group


@pytest.fixture(scope='module')
def interval():
    data = {
        "start_date": pd.Timestamp("2021-02-01T00:00:00.000Z"),
        "end_date": pd.Timestamp("2021-06-30T00:00:00.000Z"),
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
            .set_index(['level 0', 'level 1'])
            .rename({"1619740800000": datetime.date(2021, 4, 30)}, axis=1)
        )

    report_df = (
        BalanceReport(wire, simple_balance_group, interval)
        .create_report_df()
        .sort_by_group()
        .drop_zero_rows()
        .calculate_saldo()
        .get_report_df()
    )
    temp = report_df.round(0).astype(int)
    log(f'\n{temp}')
    pd.testing.assert_frame_equal(expected, report_df)


def test_create_complex_balance(wire: Wire, complex_balance_group: BalanceGroup, interval: Interval):
    report_df = (
        BalanceReport(wire, complex_balance_group, interval)
        .create_report_df()
        # .sort_by_group()
        .drop_zero_rows()
        .calculate_saldo()
        .get_report_df()
    )
    temp = report_df.round(0).astype(int)
    log(f'\n{temp.to_string()}')
