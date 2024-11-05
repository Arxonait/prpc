import datetime
import json
import uuid
from typing import Any

import jsonpickle
import pydantic

from main.support_module.exceptions import PRPCMessageDeserializeError, JSONDeserializeError
from main.support_module.loggs import Logger
from main.handlers_type import CheckerValueSerialize

logger = Logger.get_instance()
logger = logger.prpc_logger


class ValidaterPRPCMessage(pydantic.BaseModel):
    func_name: str
    func_args: list | tuple = []
    func_kwargs: dict = {}
    message_id: uuid.UUID


class PRPCMessage:
    def __init__(self, func_name: str,
                 func_args: list | tuple | None,
                 func_kwargs: dict | None,
                 message_id: uuid.UUID | None = None):

        func_args: list = func_args if func_args is not None else []
        func_kwargs: dict = func_kwargs if func_kwargs is not None else {}
        message_id: uuid.UUID = uuid.uuid4() if message_id is None else message_id

        ValidaterPRPCMessage(func_name=func_name, func_args=func_args, func_kwargs=func_kwargs, message_id=message_id)

        self.func_name: str = func_name
        self.func_args = func_args
        self.func_kwargs = func_kwargs
        self.message_id = message_id
        self.date_create_message: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)

        self.result: Any = None
        self.exception_info: str | None = None
        self.date_done_message: datetime.datetime | None = None

    def message_to_done(self, exception_info: str | None = None, result: Any = None):
        assert exception_info is not None or result is not None, "to convert PRPCMessage to done, exception_info or result must be not None"

        self.result: Any = result
        self.exception_info: str | None = exception_info
        self.date_done_message = datetime.datetime.now(datetime.timezone.utc)

    def is_message_done(self):
        if self.date_done_message is None:
            return False
        return True

    def serialize(self, serialize_raw=False) -> str:
        if serialize_raw:
            return jsonpickle.dumps(self, False)

        if self.is_message_done():
            result, wrong_values = CheckerValueSerialize().is_value_good_for_serialize(self.result)
        else:
            result_args, wrong_values_args = CheckerValueSerialize().is_value_good_for_serialize(self.func_args)
            result_kwargs, wrong_values_kwargs = CheckerValueSerialize().is_value_good_for_serialize(self.func_kwargs)
            result = result_args and result_kwargs
            wrong_values = wrong_values_args + wrong_values_kwargs
        if not result:
            logger.warning(f"Объекты {wrong_values} не возможно будет востановить на сервере/клиенте и будут восприниматься как dict")
            logger.warning(f"Возможно востановить объекты примитивных типов")

        return jsonpickle.dumps(self)

    @classmethod
    def deserialize(cls, serialize_message: str):
        try:
            message = jsonpickle.loads(serialize_message)
        except Exception as e:
            raise JSONDeserializeError

        if not isinstance(message, PRPCMessage):
            raise PRPCMessageDeserializeError(serialize_message)
        return message

    @classmethod
    def deserialize_raw(cls, serialize_message: str):
        try:
            serialize_message = json.loads(serialize_message)
        except Exception as e:
            raise JSONDeserializeError

        try:
            message = ValidaterPRPCMessage(**serialize_message)
        except pydantic.ValidationError as e:
            raise PRPCMessageDeserializeError(serialize_message)

        message = PRPCMessage(message.func_name, message.func_args, message.func_kwargs, message.message_id)
        return message

    def __str__(self):
        if self.is_message_done():
            result = f"'Object PRPCMessage done - task_id={self.message_id}, func_name='{self.func_name}', result={self.result}, exception_info={self.exception_info}'"
        else:
            result = f"'Object PRPCMessage done - task_id={self.message_id}, func_name='{self.func_name}', func_args={self.func_args}, func_kwargs={self.func_kwargs}'"
        return result
