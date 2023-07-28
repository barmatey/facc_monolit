# import typing
#
# import pandas as pd
# from sqlalchemy import Integer, ForeignKey, String
# from sqlalchemy.orm import Mapped, mapped_column
#
# from src.report import entities as e_report
# from src.sheet import entities as e_sheet
#
# from . import db
# from .category import Category, CategoryEnum
# from .sheet import Sheet
# from .group import Group
# from repository_postgres_new.source import Source
# from repository_postgres_new.interval import Interval, IntervalRepo
# from .base import BaseRepo, BaseModel
# from .sheet import SheetRepo
#
# OrderBy = typing.Union[str, list[str]]
#
#
#
#
# class ReportRepo(BaseRepo):
#     model = Report
#     sheet_repo = SheetRepo
#     interval_repo = IntervalRepo
#
#     async def create(self, data: e_report.ReportCreate) -> e_report.Report:
#         async with db.get_async_session() as session:
#             # Create sheet model
#             sheet_data = e_sheet.SheetCreate(
#                 df=data.sheet.dataframe,
#                 drop_index=data.sheet.drop_index,
#                 drop_columns=data.sheet.drop_columns,
#                 readonly_all_cells=data.sheet.readonly_all_cells,
#             )
#             sheet_id = await self.sheet_repo().create_with_session(session, sheet_data)
#
#             # Create interval model
#             interval: Interval = await self.interval_repo().create_with_session(session, data.interval)
#
#             # Create report model
#             report_data = dict(
#                 title=data.title,
#                 category_id=CategoryEnum[data.category].value,
#                 source_id=data.source_id,
#                 group_id=data.group_id,
#                 interval_id=interval.id,
#                 sheet_id=sheet_id,
#             )
#             report: Report = await self.create_with_session(session, report_data)
#             session.expunge_all()
#             await session.commit()
#             return report.to_entity(interval.to_entity())
#
#     async def retrieve(self, filter_by: dict):
#         async with db.get_async_session() as session:
#             report: Report = await self.retrieve_with_session(session, filter_by)
#             interval: Interval = await self.interval_repo().retrieve_with_session(session, {"id": report.interval_id})
#             return report.to_entity(interval.to_entity())
#

#     async def overwrite_linked_sheet(self, instance: e_report.Report, data: e_report.SheetCreate) -> None:
#         async with db.get_async_session() as session:
#             sheet_data = e_sheet.SheetCreate(
#                 df=data.dataframe,
#                 drop_index=data.drop_index,
#                 drop_columns=data.drop_columns,
#                 readonly_all_cells=data.readonly_all_cells
#             )
#             await self.sheet_repo().overwrite_with_session(session, instance.sheet_id, sheet_data)
#             await session.commit()
