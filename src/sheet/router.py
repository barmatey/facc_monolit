from fastapi import APIRouter
from fastapi.responses import JSONResponse

from repository_postgres_new.sheet import SheetRepoPostgres
from src import db, core_types, helpers
from . import schema, entities
from .service import SheetService

router = APIRouter(
    prefix="/sheet",
    tags=['Sheet']
)


@router.get("/{sheet_id}")
@helpers.async_timeit
async def retrieve_sheet(sheet_id: core_types.Id_, from_scroll: int = None, to_scroll: int = None, ) -> JSONResponse:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)

        sheet_retrieve_schema = schema.SheetRetrieveSchema(
            sheet_id=sheet_id,
            from_scroll=from_scroll,
            to_scroll=to_scroll
        )
        sheet = await sheet_service.get_one(sheet_retrieve_schema)
        return JSONResponse(content=sheet)


@router.get("/{sheet_id}/retrieve-scroll-size")
@helpers.async_timeit
async def retrieve_scroll_size(sheet_id: core_types.Id_, ) -> schema.ScrollSizeSchema:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        scroll_size = await sheet_service.get_scroll_size(sheet_id)
        return scroll_size


@router.get("/{sheet_id}/retrieve-unique-cells")
@helpers.async_timeit
async def retrieve_col_filter(sheet_id: core_types.Id_, col_id: core_types.Id_, ) -> schema.ColFilterSchema:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        retrieve_schema = schema.ColFilterRetrieveSchema(sheet_id=sheet_id, col_id=col_id, )
        col_filter = await sheet_service.get_col_filter(retrieve_schema)
        return col_filter


@router.patch("/{sheet_id}/update-col-filter")
@helpers.async_timeit
async def update_col_filter(sheet_id: core_types.Id_, data: schema.ColFilterSchema,) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.update_col_filter(data)
        return 1


@router.delete("/{sheet_id}/clear-all-filters")
@helpers.async_timeit
async def clear_all_filters(sheet_id: core_types.Id_,) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.clear_all_filters(sheet_id)
        return 1


@router.patch("/{sheet_id}/update-col-sorter")
@helpers.async_timeit
async def update_col_sorter(sheet_id: core_types.Id_, data: schema.ColSorterSchema,) -> JSONResponse:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.update_col_sorter(data)
        sheet_retrieve_schema = schema.SheetRetrieveSchema(
            sheet_id=sheet_id,
            from_scroll=data.from_scroll,
            to_scroll=data.to_scroll,
        )
        sheet_schema = await sheet_service.get_one(sheet_retrieve_schema)
        return JSONResponse(content=sheet_schema)


@router.patch("/{sheet_id}/update-col-width")
@helpers.async_timeit
async def update_col_width(sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.update_col_size(sheet_id, data)
        return 1


@router.patch("/{sheet_id}/update-cell")
@helpers.async_timeit
async def update_cell(sheet_id: core_types.Id_, data: entities.Cell,) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.update_cell(sheet_id, data)
        return 1


@router.patch("/{sheet_id}/update-cell-bulk")
@helpers.async_timeit
async def update_cell_bulk(sheet_id: core_types.Id_, data: list[entities.Cell],) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.update_cell_many(sheet_id, data)
        return 1


@router.delete("/{sheet_id}/delete-rows")
@helpers.async_timeit
async def delete_rows(sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> int:
    async with db.get_async_session() as session:
        sheet_repo = SheetRepoPostgres(session)
        sheet_service = SheetService(sheet_repo)
        await sheet_service.delete_row_many(sheet_id, row_ids)
        return 1
