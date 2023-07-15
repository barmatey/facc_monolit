import loguru
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from . import schema

import core_types
import helpers
from . import schema
from . import service

router = APIRouter(
    prefix="/sheet",
    tags=['Sheet']
)


@router.get("/{sheet_id}")
@helpers.async_timeit
async def retrieve(sheet_id: core_types.Id_,
                   from_scroll: int = None,
                   to_scroll: int = None,
                   sheet_service: service.SheetService = Depends(service.SheetService)
                   ) -> schema.SheetSchema:
    sheet_retrieve_schema = schema.SheetRetrieveSchema(
        sheet_id=sheet_id,
        from_scroll=from_scroll,
        to_scroll=to_scroll
    )
    sheet_schema = await sheet_service.retrieve_sheet(sheet_retrieve_schema)
    return sheet_schema


@router.get("/{sheet_id}/retrieve-scroll-size")
@helpers.async_timeit
async def retrieve_scroll_size(sheet_id: core_types.Id_,
                               sheet_service: service.SheetService = Depends(service.SheetService)
                               ) -> schema.ScrollSizeSchema:
    scroll_size = await sheet_service.retrieve_scroll_size(sheet_id)
    return scroll_size


@router.get("/{sheet_id}/retrieve-unique-cells")
async def retrieve_col_filter(sheet_id: core_types.Id_, col_id: core_types.Id_,
                              sheet_service: service.SheetService = Depends(service.SheetService),
                              ) -> schema.ColFilterSchema:
    retrieve_schema = schema.ColFilterRetrieveSchema(sheet_id=sheet_id, col_id=col_id, )
    col_filter = await sheet_service.retrieve_col_filter(retrieve_schema)
    return col_filter


@router.patch("/{sheet_id}/update-col-filter")
async def update_col_filter(sheet_id: core_types.Id_, data: schema.ColFilterSchema,
                            sheet_service: service.SheetService = Depends(service.SheetService), ) -> int:
    await sheet_service.update_col_filter(data)
    return 1


@router.delete("/{sheet_id}/clear-all-filters")
async def clear_all_filters(sheet_id: core_types.Id_,
                            sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.clear_all_filters(sheet_id)
    return 1


@router.patch("/{sheet_id}/update-col-sorter")
async def update_col_sorter(data: schema.ColSorterSchema,
                            sheet_service: service.SheetService = Depends(service.SheetService)) -> JSONResponse:
    await sheet_service.update_col_sorter(data)
    sheet_retrieve_schema = schema.SheetRetrieveSchema(
        sheet_id=data.sheet_id,
        from_scroll=data.from_scroll,
        to_scroll=data.to_scroll,
    )
    sheet_schema = await sheet_service.retrieve_sheet(sheet_retrieve_schema)
    return JSONResponse(content=sheet_schema)


@router.patch("/{sheet_id}/copy-rows")
async def copy_rows(sheet_id: core_types.Id_,
                    copy_from: list[schema.CopySindexSchema],
                    copy_to: list[schema.CopySindexSchema],
                    sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.copy_rows(sheet_id, copy_from, copy_to)
    return 1


@router.patch("/{sheet_id}/copy-cols")
async def copy_cols(sheet_id: core_types.Id_,
                    copy_from: list[schema.CopySindexSchema],
                    copy_to: list[schema.CopySindexSchema],
                    sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.copy_cols(sheet_id, copy_from, copy_to)
    return 1


@router.patch("/{sheet_id}/copy-cells")
async def copy_cells(sheet_id: core_types.Id_,
                     copy_from: list[schema.CopyCellSchema],
                     copy_to: list[schema.CopyCellSchema],
                     sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.copy_cells(sheet_id, copy_from, copy_to, )
    return 1


@router.patch("/{sheet_id}/update-col-width")
async def update_col_width(sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema,
                           sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.update_col_size(sheet_id, data)
    return 1


@router.patch("/{sheet_id}/update-cell")
async def update_cell(sheet_id: core_types.Id_, data: schema.UpdateCellSchema,
                      sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.update_cell(sheet_id, data)
    return 1


@router.delete("/{sheet_id}/delete-rows")
async def delete_rows(sheet_id: core_types.Id_, row_ids: list[core_types.Id_],
                      sheet_service: service.SheetService = Depends(service.SheetService)) -> int:
    await sheet_service.delete_rows(sheet_id, row_ids)
    return 1
