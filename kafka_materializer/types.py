from typing import List
from ._base import BaseModel


class BaseView(BaseModel):
    topic: str
    order_by: str
    fields: List[str]


class View(BaseView):
    id: str
