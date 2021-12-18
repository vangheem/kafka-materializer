from typing import Any, Dict, List, Optional
import uuid
from fastapi import APIRouter, FastAPI, Request, Depends
from pydantic.main import BaseModel
from kafka_materializer.data.materializer import MaterializerDataService
from kafka_materializer.settings import Settings
from .types import BaseView, View

router = APIRouter()


def materializer_service(request: Request) -> MaterializerDataService:
    return request.app.materializer_service


@router.post("/views")
def create_view(
    view_data: BaseView,
    materializer_service: MaterializerDataService = Depends(materializer_service),
) -> View:
    view = View(id=uuid.uuid4().hex, **view_data.dict())
    materializer_service.create_view(view)
    return view


class PagedViewsResult(BaseModel):
    results: List[View]
    cursor: Optional[str] = None


@router.get("/views")
def get_views(
    materializer_service: MaterializerDataService = Depends(materializer_service),
    cursor: Optional[str] = None,
) -> PagedViewsResult:
    cursor, results = materializer_service.get_views(cursor=cursor)
    return PagedViewsResult(cursor=cursor, results=results)


@router.delete("/views/{id}")
def delete_view(
    id: str,
    materializer_service: MaterializerDataService = Depends(materializer_service),
) -> None:
    materializer_service.delete_view(id)


class PagedQueryResult(BaseModel):
    results: List[Dict[str, Any]]
    cursor: Optional[str] = None


@router.get("/views/{id}/query")
def view_query(
    id: str,
    cursor: Optional[str] = None,
    materializer_service: MaterializerDataService = Depends(materializer_service),
) -> PagedQueryResult:
    cursor, results = materializer_service.query_view(id, cursor=cursor)
    return PagedQueryResult(cursor=cursor, results=results)


class API(FastAPI):
    materializer_service: MaterializerDataService

    def __init__(self, settings: Settings, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._settings = settings

        self.include_router(router)

        self.add_event_handler("startup", self.initialize)
        self.add_event_handler("shutdown", self.finalize)

    def initialize(self):
        self.materializer_service = MaterializerDataService(self._settings)

    def finalize(self):
        self.materializer_service.close()
