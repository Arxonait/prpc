import datetime
import logging
import uuid
from typing import Any

import jsonpickle

from main.exceptions import PRPCMessageDeserializeError
from main.type_module import CheckerValueSerialize, BASE_MODULE, LIB_MODULE


class PRPCMessage:
    def __init__(self, func_name, func_args, func_kwargs, message_id=None, date_create_message=None,
                 result=None,
                 exception_info=None,
                 date_done_message=None):

        self.result = result
        self.exception_info = exception_info
        self.date_done_message = date_done_message

        self.func_name: str = func_name
        self.func_args: list = func_args
        self.func_kwargs: dict = func_kwargs

        self.message_id: uuid.UUID = uuid.uuid4() if message_id is None else message_id
        self.date_create_message: datetime.datetime = datetime.datetime.now(datetime.timezone.utc) if date_create_message is None else date_create_message

    def message_to_done(self, exception_info=None, result=None):
        assert exception_info is not None or result is not None, "to convert PRPCMessage to done, exception_info or result must be not None"

        self.result: Any = result
        self.exception_info: str | None = exception_info
        self.date_done_message = datetime.datetime.now(datetime.timezone.utc)

    def is_message_done(self):
        if self.date_done_message is None:
            return False
        return True

    def serialize(self):
        if self.is_message_done():
            result, wrong_values = CheckerValueSerialize().is_value_good_for_serialize(self.result)
        else:
            result_args, wrong_values_args = CheckerValueSerialize().is_value_good_for_serialize(self.func_args)
            result_kwargs, wrong_values_kwargs = CheckerValueSerialize().is_value_good_for_serialize(self.func_kwargs)
            result = result_args and result_kwargs
            wrong_values = wrong_values_args + wrong_values_kwargs
        if not result:
            logging.warning(f"Объекты {wrong_values} не возможно будет востановить на сервере/клиенте и будут восприниматься как dict")
            logging.warning(f"Возможно востановить объекты модулей {BASE_MODULE} и {LIB_MODULE}, а также конректных типов {CheckerValueSerialize.specific_type}")

        return jsonpickle.dumps(self)

    @classmethod
    def deserialize(cls, serialize_message):
        #logging.debug(f"Началась сериализация данных {serialize_message}")
        message = jsonpickle.loads(serialize_message)
        if not isinstance(message, PRPCMessage):
            raise PRPCMessageDeserializeError(serialize_message)
        #logging.debug(f"Закончилась сериализация данных")
        return message

    def __str__(self):
        if self.is_message_done():
            result = f"'Object PRPCMessage done - task_id={self.message_id}, func_name='{self.func_name}', result={self.result}, exception_info={self.exception_info}'"
        else:
            result = f"'Object PRPCMessage done - task_id={self.message_id}, func_name='{self.func_name}', func_args={self.func_args}, func_kwargs={self.func_kwargs}'"
        return result
