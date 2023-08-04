import typing
import pandas as pd

from src.core_types import Event, Id_
from . import entities


class IntervalCreated(Event):
    period_year: int
    period_month: int
    period_day: int
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]

    class Config:
        arbitrary_types_allowed = True


class ReportCreated(Event):
    title: str
    interval: IntervalCreated
    category: entities.InnerCategory
    source: entities.InnerSource
    group: entities.InnerGroup
    sheet: typing.Optional[entities.InnerSheet]


class ReportGotten(Event):
    report_id: Id_


class ReportListGotten(Event):
    category: entities.InnerCategory


class ReportParentUpdated(Event):
    pass