import typing
import pandas as pd
from datetime import datetime

from src import core_types
from src.core_types import Event, Id_
from . import entities


class IntervalCreated(Event):
    period_year: int
    period_month: int
    period_day: int
    start_date: datetime
    end_date: datetime
    total_start_date: typing.Optional[datetime] = None
    total_end_date: typing.Optional[datetime] = None


class ReportCreated(Event):
    title: str
    interval: IntervalCreated
    category: entities.InnerCategory
    source: entities.InnerSource
    group: entities.InnerGroup
    sheet: typing.Optional[entities.InnerSheet] = None


class ReportGotten(Event):
    report_id: Id_


class ReportListGotten(Event):
    category: entities.InnerCategory


class ReportDeleted(Event):
    report_id: core_types.Id_


class ParentUpdated(Event):
    report_instance: entities.Report


class ReportSheetUpdated(Event):
    report_instance: entities.Report


class ReportCheckerCreated(Event):
    report_instance: entities.Report
