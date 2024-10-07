import datetime
import uuid
import json
from typing import Any

from pydantic import BaseModel


class Task(BaseModel):
    name_func: str
    func_args: list
    func_kwargs: dict
    result: Any = None

    task_id: uuid.UUID = uuid.uuid4()
    date_create_task: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)