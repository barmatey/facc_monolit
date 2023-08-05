from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from src.repository_postgres_new.sheet import SheetRepoPostgres
from src import db, core_types, helpers
from src.messagebus import messagebus
from . import schema, entities, events
from .service import SheetService

router = APIRouter(
    prefix="/sheet",
    tags=['Sheet']
)


@router.get("/{sheet_id}")
@helpers.async_timeit
async def get_one_sheet(sheet_id: core_types.Id_, from_scroll: int = None, to_scroll: int = None,
                        get_asession=Depends(db.get_async_session)) -> JSONResponse:
    async with get_asession as session:
        event = events.SheetGotten(sheet_id=sheet_id, from_scroll=from_scroll, to_scroll=to_scroll)
        sheet: entities.Sheet = await messagebus.handle(event, session)
        await session.commit()
        return JSONResponse(content=sheet)


@router.get("/{sheet_id}/retrieve-unique-cells")
@helpers.async_timeit
async def get_col_filter(sheet_id: core_types.Id_, col_id: core_types.Id_,
                         get_asession=Depends(db.get_async_session)) -> entities.ColFilter:
    async with get_asession as session:
        event = events.ColFilterGotten(sheet_id=sheet_id, col_id=col_id)
        col_filter: entities.ColFilter = await messagebus.handle(event, session)
        await session.commit()
        return col_filter


@router.patch("/{sheet_id}/update-col-filter")
@helpers.async_timeit
async def update_col_filter(sheet_id: core_types.Id_, data: entities.ColFilter,
                            get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.ColFilterUpdated(sheet_id=sheet_id, col_filter=data)
        await messagebus.handle(event, session)
        await session.commit()
        return 1


@router.delete("/{sheet_id}/clear-all-filters")
@helpers.async_timeit
async def clear_all_filters(sheet_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.ColFiltersDropped(sheet_id=sheet_id)
        await messagebus.handle(event, session)
        await session.commit()
        return 1


@router.patch("/{sheet_id}/update-col-sorter")
@helpers.async_timeit
async def update_col_sorter(sheet_id: core_types.Id_, data: entities.ColSorter,
                            get_asession=Depends(db.get_async_session)) -> JSONResponse:
    async with get_asession as session:
        event = events.ColSortedUpdated(sheet_id=sheet_id, col_sorter=data)
        sheet = await messagebus.handle(event, session)
        await session.commit()
        return JSONResponse(content=sheet)


@router.patch("/{sheet_id}/update-col-width")
@helpers.async_timeit
async def update_col_width(sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema,
                           get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.ColWidthUpdated(sheet_id=sheet_id, new_size=data.new_size, sindex_id=data.sindex_id)
        await messagebus.handle(event, session)
        await session.commit()
        return 1


@router.patch("/{sheet_id}/update-cell")
@helpers.async_timeit
async def partial_update_cell(sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema,
                              get_asession=Depends(db.get_async_session)) -> JSONResponse:
    async with get_asession as session:
        try:
            event = events.CellsPartialUpdated(sheet_id=sheet_id, cells=[data])
            await messagebus.handle(event, session)
            await session.commit()
            return JSONResponse(content=1)
        except LookupError:
            return JSONResponse(status_code=status.HTTP_423_LOCKED, content={})


@router.patch("/{sheet_id}/update-cell-bulk")
@helpers.async_timeit
async def partial_update_many_cells(sheet_id: core_types.Id_, data: list[schema.PartialUpdateCellSchema],
                                    get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.CellsPartialUpdated(sheet_id=sheet_id, cells=data)
        await messagebus.handle(event, session)
        await session.commit()
        return 1


@router.patch("/{sheet_id}/delete-rows")
@helpers.async_timeit
async def delete_rows(sheet_id: core_types.Id_, row_ids: list[core_types.Id_],
                      get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.RowsDeleted(sheet_id=sheet_id, row_ids=row_ids)
        await messagebus.handle(event, session)
        await session.commit()
        return 1
