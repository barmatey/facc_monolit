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
