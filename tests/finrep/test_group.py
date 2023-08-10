import pandas as pd
import pytest
from pathlib import Path

from src.finrep.wire import Wire
from src.finrep.group import BalanceGroup
from src.helpers import log

SARMAT_PATH = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/sarmat.csv")
SIMPLE_BALANCE_GROUP_PATH = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_group.csv")
SB_RENAMED = Path("C:/Users/barma/PycharmProjects/facc_monolit/tests/files/simple_balance_group_with_renamed_items.csv")


@pytest.fixture(scope='module')
def wire_df():
    df = pd.read_csv(SARMAT_PATH, encoding='utf-8')
    return df


def test_create_simple_balance_group(wire_df):
    wire = Wire(wire_df)
    real_df: pd.DataFrame = BalanceGroup.from_wire(wire, ccols=['sender']).get_group_df()
    expected_df: pd.DataFrame = pd.read_csv(SIMPLE_BALANCE_GROUP_PATH)
    pd.testing.assert_frame_equal(expected_df, real_df)


def test_update_simple_balance_group(wire_df):
    wire = Wire(wire_df)
    old_group_df = BalanceGroup.from_wire(wire, ccols=['sender']).get_group_df().head(5)

    expected_updated_df: pd.DataFrame = pd.read_csv(SIMPLE_BALANCE_GROUP_PATH)
    real_updated_df: pd.DataFrame = BalanceGroup(old_group_df, ccols=['sender']).update_group_df(wire).get_group_df()
    pd.testing.assert_frame_equal(expected_updated_df, real_updated_df)


def test_update_simple_balance_group_with_fixed_cols(wire_df):
    wire = Wire(wire_df)
    old_group_df = BalanceGroup.from_wire(wire, ccols=['sender']).get_group_df().head(5)
    expected_updated_df: pd.DataFrame = pd.read_csv(SIMPLE_BALANCE_GROUP_PATH).head(5)
    real_updated_df: pd.DataFrame = (BalanceGroup(old_group_df, ccols=['sender'], fixed_ccols=['sender'])
                                     .update_group_df(wire)
                                     .get_group_df()
                                     )

    pd.testing.assert_frame_equal(expected_updated_df, real_updated_df)


def test_update_simple_balance_group_with_renaming_items(wire_df):
    wire = Wire(wire_df)
    expected_group_df = pd.read_csv(SB_RENAMED).apply(lambda col: pd.to_numeric(col, errors='ignore'), axis=1)
    old_group_df = expected_group_df.head(8)
    real_group_df = BalanceGroup(old_group_df, ccols=['sender']).update_group_df(wire).get_group_df()
    assert expected_group_df.to_json(orient='records') == real_group_df.to_json(orient='records')
