from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

import core_types
from . import schema
from . import service

router = APIRouter(
    prefix="/sheet",
    tags=['Sheet']
)


@router.get("/{sheet_id}")
async def retrieve(sheet_id: core_types.Id_,
                   from_scroll: int = None,
                   to_scroll: int = None,
                   sheet_service: service.SheetService = Depends(service.SheetService)
                   ) -> JSONResponse:
    sheet_retrieve_schema = schema.SheetRetrieveSchema(
        sheet_id=sheet_id,
        from_scroll=from_scroll,
        to_scroll=to_scroll
    )
    sheet_schema = await sheet_service.retrieve_sheet(sheet_retrieve_schema)
    return JSONResponse(content=sheet_schema)


@router.get("/{sheet_id}/retrieve-scroll-size")
async def retrieve_scroll_size(sheet_id: core_types.Id_,
                               sheet_service: service.SheetService = Depends(service.SheetService)
                               ) -> schema.ScrollSizeSchema:
    scroll_size = await sheet_service.retrieve_scroll_size(sheet_id)
    return scroll_size


@router.get("/{sheet_id}/retrieve-unique-cells")
async def retrieve_col_filter(sheet_id: core_types.Id_, col_id: core_types.Id_,
                              sheet_service: service.SheetService = Depends(service.SheetService),
                              ) -> schema.ColFilterSchema:
    col_filter = await sheet_service.retrieve_col_filter(col_id=col_id)
    return col_filter


@router.patch("/{sheet_id}/update-col-filter")
async def update_col_filter(sheet_id: core_types.Id_, data: schema.ColFilterSchema,
                            sheet_service: service.SheetService = Depends(service.SheetService), ) -> int:
    await sheet_service.update_col_filter(data)
    return 33
