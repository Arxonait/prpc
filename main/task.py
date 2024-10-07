import datetime
import uuid
import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field


class Task(BaseModel):
    name_func: str
    func_args: list
    func_kwargs: dict

    task_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    date_create_task: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)


class TaskDone(Task):
    result: Any = None
    exception_info: str | None = None
    date_done_task: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)


def task_to_task_done(task: Task, exception_info=None, result=None):
    task = TaskDone(**task.model_dump(), exception_info=exception_info, result=result)
    return task
