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


async def handle_wire_many_created(hs: HS, event: wire_events.WireManyCreated):
    await hs.wire_service.create_many(event.wires)
    hs.results[wire_events.WireManyCreated] = 1

    hs.queue.append(wire_events.SourceDatesInfoUpdated(source_id=event.source_id))


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
    wire_events.WireManyCreated: [handle_wire_many_created],
    wire_events.SourceDatesInfoUpdated: [handle_source_info_updated]
}
