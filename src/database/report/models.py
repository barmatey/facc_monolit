# from datetime import datetime
# from sqlalchemy import ForeignKey, MetaData, Table, Column, String, JSON, Integer, Float, TIMESTAMP
#
# metadata = MetaData()
#
# Category = Table(
#     'category',
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("title", String(80)),
# )
#
# Group = Table(
#     'group',
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("title", String(80)),
#     Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE')),
#     Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE')),
#     Column("sheet_id", Integer, ForeignKey("sheet.id", ondelete='PROTECTED')),
# )
#
# Report = Table(
#     'report',
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE')),
#     Column("group_id", Integer, ForeignKey("group.id", ondelete='CASCADE')),
#     Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE')),
#     Column("sheet_id", Integer, ForeignKey("sheet.id", ondelete='PROTECTED')),
# )
#
