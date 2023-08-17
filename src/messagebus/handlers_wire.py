import loguru
import pandas as pd

from src import core_types

from src.wire import events as wire_events
from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities
from src.group import entities as group_entities
from src.sheet import entities as sheet_entities
from src.wire import entities as wire_entities

from .handler_service import HandlerService as HS


async def handle_source_created(hs: HS, event: wire_events.SourceCreated):
    source: wire_entities.Source = await hs.source_service.create_one(event)
    hs.results[wire_events.SourceCreated] = source


async def handle_wire_many_created(hs: HS, event: wire_events.WireManyCreated):
    await hs.wire_service.create_many(event.wires)
    hs.results[wire_events.WireManyCreated] = 1

    hs.queue.append(wire_events.SourceDatesInfoUpdated(source_id=event.source_id))


async def handle_plan_items_created_from_source(hs: HS, event: wire_events.PlanItemsCreatedFromSource):
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.source_id})
    await hs.source_plan_service.create_many_from_wire_df(event.source_id, wire_df)
    hs.results[wire_events.PlanItemsCreatedFromSource] = 1


async def handle_plan_item_list_gotten(hs: HS, event: wire_events.PlanItemListGotten):
    filter_by = event.model_dump(exclude_none=True)
    order_by = ['sender', 'receiver', 'sub1', 'sub2', ]
    result = await hs.source_plan_service.get_many(filter_by, order_by)
    hs.results[wire_events.PlanItemListGotten] = result


async def handle_plan_item_deleted(hs: HS, event: wire_events.PlanItemDeleted):
    await hs.source_plan_service.delete_many(event.filter_by)
    hs.results[wire_events.PlanItemDeleted] = None


# todo I need update source total_start_date and total_end_date
async def handle_wire_partial_updated(hs: HS, event: wire_events.WirePartialUpdated):
    data = event.model_dump()
    filter_by = {"id": data.pop("wire_id")}
    wire: wire_entities.Wire = await hs.wire_service.update_one(data, filter_by)
    hs.results[wire_events.WirePartialUpdated] = wire


# todo dates are hardcoding!
async def handle_source_info_updated(hs: HS, event: wire_events.SourceDatesInfoUpdated):
    total_start_date = pd.Timestamp("2020-12-01T09:07:13.363Z")
    total_end_date = pd.Timestamp("2022-11-01T09:07:13.363Z")
    data = {
        "total_start_date": total_start_date,
        "total_end_date": total_end_date,
    }
    filter_by = {"id": event.source_id}
    source = await hs.source_service.update_one(data, filter_by)

    hs.results[wire_events.SourceDatesInfoUpdated] = source


HANDLERS_WIRE = {
    wire_events.SourceCreated: [handle_source_created],
    wire_events.SourceDatesInfoUpdated: [handle_source_info_updated],
    wire_events.PlanItemsCreatedFromSource: [handle_plan_items_created_from_source],
    wire_events.PlanItemListGotten: [handle_plan_item_list_gotten],
    wire_events.PlanItemDeleted: [handle_plan_item_deleted],
    wire_events.WireManyCreated: [handle_wire_many_created],
    wire_events.WirePartialUpdated: [handle_wire_partial_updated],

}
