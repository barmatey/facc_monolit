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
