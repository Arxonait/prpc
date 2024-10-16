import datetime
import uuid
from typing import Any

import jsonpickle
from pydantic import BaseModel, Field


class Task:
    def __init__(self, func_name, func_args, func_kwargs, task_id=None, date_create_task=None,
                 result=None,
                 exception_info=None,
                 date_done_task=None):
        self.result = result
        self.exception_info = exception_info
        self.date_done_task = date_done_task

        self.func_name: str = func_name
        self.func_args: list = func_args
        self.func_kwargs: dict = func_kwargs

        self.task_id: uuid.UUID = uuid.uuid4() if task_id is None else task_id
        self.date_create_task: datetime.datetime = datetime.datetime.now(datetime.timezone.utc) if date_create_task is None else date_create_task

    def task_to_done(self, exception_info=None, result=None):
        if exception_info is None and result is None:
            raise Exception # todo

        self.result: Any = result
        self.exception_info: str | None = exception_info
        self.date_done_task = datetime.datetime.now(datetime.timezone.utc)

    def is_task_done(self):
        if self.date_done_task is None:
            return False
        return True

    def serialize(self):
        return jsonpickle.dumps(self)

    @classmethod
    def deserialize(cls, serialize_task):
        return jsonpickle.loads(serialize_task)

    def __str__(self):
        if self.is_task_done():
            result = f"'Object Task - task done - task_id={self.task_id}, func_name='{self.func_name}', result={self.result}, exception_info={self.exception_info}'"
        else:
            result = f"'Object Task - task done - task_id={self.task_id}, func_name='{self.func_name}', func_args={self.func_args}, func_kwargs={self.func_kwargs}'"
        return result
