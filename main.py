import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.wire.router import router_wire, router_source
from src.sheet.router import router as router_sheet
from src.report.router import router_report, router_group, router_category

app = FastAPI()

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router_source)
app.include_router(router_wire)
app.include_router(router_group)
app.include_router(router_report)
app.include_router(router_category)
app.include_router(router_sheet)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9999)
