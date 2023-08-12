import loguru

from src import core_types

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities
from src.group import entities as group_entities
from src.sheet import entities as sheet_entities

from .handler_service import HandlerService as HS


async def handle_sheet_created(hs: HS, event: sheet_events.SheetCreated):
    sheet_id: core_types.Id_ = await hs.sheet_service.create_one(event)
    hs.results[sheet_events.SheetCreated] = sheet_id


async def handle_sheet_gotten(hs: HS, event: sheet_events.SheetGotten):
    sheet: sheet_entities.Sheet = await hs.sheet_service.get_full_sheet(event)
    hs.results[sheet_events.SheetGotten] = sheet


async def handle_sheet_info_updated(hs: HS, event: sheet_events.SheetInfoUpdated):
    loguru.logger.debug('handle_sheet_info_updated')
    sheet = await hs.sheet_service.update_sheet_info(data=event.data, filter_by={"id": event.sheet_id})
    hs.results[sheet_events.SheetInfoUpdated] = sheet


async def handle_col_filter_gotten(hs: HS, event: sheet_events.ColFilterGotten):
    col_filter: sheet_entities.ColFilter = await hs.sheet_service.get_col_filter(event)
    hs.results[sheet_events.ColFilterGotten] = col_filter


async def handle_col_filter_updated(hs: HS, event: sheet_events.ColFilterUpdated):
    await hs.sheet_service.update_col_filter(event)
    hs.results[sheet_events.ColFilterUpdated] = None


async def handle_clear_all_filters(hs: HS, event: sheet_events.ColFiltersDropped):
    await hs.sheet_service.clear_all_filters(sheet_id=event.sheet_id)
    hs.results[sheet_events.ColFiltersDropped] = None


async def handle_col_sorter_updated(hs: HS, event: sheet_events.ColFilterUpdated):
    await hs.sheet_service.update_col_sorter(event)
    event = sheet_events.SheetGotten(sheet_id=event.sheet_id)
    hs.queue.append(event)
    hs.results[sheet_events.ColFilterUpdated] = None


async def handle_col_width_updated(hs: HS, event: sheet_events.ColWidthUpdated):
    await hs.sheet_service.update_col_size(event)
    hs.results[sheet_events.ColWidthUpdated] = None


async def handle_cells_partial_updated(hs: HS, event: sheet_events.CellsPartialUpdated):
    await hs.sheet_service.update_cell_many(sheet_id=event.sheet_id, data=event.cells)
    hs.results[sheet_events.CellsPartialUpdated] = None
    hs.queue.append(sheet_events.SheetInfoUpdated(sheet_id=event.sheet_id, data={}))


async def handle_rows_deleted(hs: HS, event: sheet_events.RowsDeleted):
    await hs.sheet_service.delete_row_many(sheet_id=event.sheet_id, row_ids=event.row_ids)
    hs.results[sheet_events.RowsDeleted] = None
    hs.queue.append(sheet_events.SheetInfoUpdated(sheet_id=event.sheet_id, data={}))


HANDLERS_SHEET = {
    sheet_events.SheetInfoUpdated: [handle_sheet_info_updated],
    sheet_events.SheetCreated: [handle_sheet_created],
    sheet_events.SheetGotten: [handle_sheet_gotten],
    sheet_events.ColFilterGotten: [handle_col_filter_gotten],
    sheet_events.ColFilterUpdated: [handle_col_filter_updated],
    sheet_events.ColFiltersDropped: [handle_clear_all_filters],
    sheet_events.ColSortedUpdated: [handle_col_sorter_updated],
    sheet_events.ColWidthUpdated: [handle_col_width_updated],
    sheet_events.CellsPartialUpdated: [handle_cells_partial_updated],
    sheet_events.RowsDeleted: [handle_rows_deleted],
}
