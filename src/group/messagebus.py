from collections import deque
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

import core_types
from src.core_types import Event
from src.service_finrep import get_finrep
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres, SheetRepoPostgres
from src.sheet.service import SheetService
from src.wire.service import CrudService

from src.sheet import events as sheet_events

from .entities import Group, ExpandedGroup
from .service import GroupService
from . import events as group_events


class MessageBus:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.queue = deque()
        self.wire_service = CrudService(WireRepoPostgres(session))
        self.sheet_service = SheetService(SheetRepoPostgres(session))
        self.group_service = GroupService(GroupRepoPostgres(session))
        self._HANDLERS = self._register_handlers()

    def _register_handlers(self):
        return {
            group_events.GroupCreated: [self.handle_group_created],
            group_events.GroupGotten: [self.handle_group_gotten],
            group_events.GroupListGotten: [self.handle_group_list_gotten],
            group_events.GroupPartialUpdated: [self.handle_group_partial_updated],
            group_events.ParentSourceUpdated: [self.handle_source_updated],
            group_events.GroupSheetUpdated: [self.handle_group_sheet_updated],
            group_events.GroupDeleted: [self.handle_group_deleted],
        }

    async def handle_group_created(self, event: group_events.GroupCreated) -> Group:
        event = event.copy()

        # Create sheet
        wire_df = await self.wire_service.get_many_as_frame({"sheet_id": event.sheet_id})
        finrep = get_finrep(event.category)
        group_df = finrep.create_group(wire_df, target_columns=event.columns)
        event.sheet_id = await self.sheet_service.create_one(
            sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

        # Create group from sheet_id and other group data
        group = await self.group_service.create_one(event)
        return group

    async def handle_group_gotten(self, event: group_events.GroupGotten) -> Group:
        group = await self.group_service.get_one({"id": event.group_id})
        if group.updated_at < group.source.updated_ad:
            self.queue.append(group_events.ParentSourceUpdated(group_instance=group))
        return group

    async def handle_group_list_gotten(self, _event: group_events.GroupListGotten) -> list[Group]:
        groups = await self.group_service.get_many(filter_by={})
        return groups

    async def handle_group_partial_updated(self, event: group_events.GroupListGotten) -> Group:
        data = event.dict()
        filter_by = {"id": data.pop('id')}
        updated = await self.group_service.update_one(data, filter_by)
        return updated

    async def handle_source_updated(self, event: group_events.ParentSourceUpdated) -> ExpandedGroup:
        old_group_df = await self.sheet_service.get_one_as_frame(
            sheet_events.SheetGotten(sheet_id=event.group_instance.sheet_id))

        # Create new group df
        wire_df = await self.wire_service.get_many_as_frame({"source_id": event.group_instance.source_id})
        finrep = get_finrep(event.group_instance.category)
        new_group_df = finrep.create_group(wire_df, target_columns=event.group_instance.columns)
        if len(event.group_instance.fixed_columns):
            new_group_df = pd.merge(
                old_group_df[event.group_instance.fixed_columns],
                new_group_df,
                on=event.group_instance.fixed_columns, how='left'
            )

        # Update sheet with new group df
        await self.sheet_service.overwrite_one(
            sheet_id=event.group_instance.sheet_id,
            data=sheet_events.SheetCreated(df=new_group_df, drop_index=True, drop_columns=False)
        )

        self.queue.append(group_events.GroupSheetUpdated(group_id=event.group_instance.id))

        result = ExpandedGroup(**event.group_instance.dict())
        result.sheet_df = new_group_df
        return result

    async def handle_group_sheet_updated(self, event: group_events.GroupSheetUpdated):
        _ = await self.group_service.update_one({}, filter_by={"id": event.group_id})

    async def handle_group_deleted(self, event: group_events.GroupDeleted) -> core_types.Id_:
        deleted_id = await self.group_service.delete_one(filter_by={"id": event.group_id})
        return deleted_id

    async def handle(self, event: Event) -> list:
        self.queue.append(event)
        results = []
        while self.queue:
            event = self.queue.popleft()
            for handler in self._HANDLERS[type(event)]:
                results.append(await handler(event))
        return results
