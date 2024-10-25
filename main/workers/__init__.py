import asyncio
from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Literal

from main.prpcmessage import PRPCMessage


class WorkerType(Enum):
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"


WORKER_TYPE_ANNOTATE = Literal["thread", "process", "async"]


class Worker(ABC):

    def __init__(self, message: PRPCMessage, func: Callable, timeout: timedelta = None):
        self.message = message
        self.timeout = timeout
        self.func = func

        self._time_start_work: datetime | None = None

    @abstractmethod
    def get_future(self) -> asyncio.Future | asyncio.Task:
        raise NotImplementedError

    @abstractmethod
    def start_work(self):
        raise NotImplementedError

    @abstractmethod
    def stop_work(self):
        raise NotImplementedError

    @abstractmethod
    def _check_done_of_concurrence_obj(self):
        raise NotImplementedError

    @abstractmethod
    def _get_result_of_concurrence_obj(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def check_ability_to_work_with_function(cls, func_data):
        raise NotImplementedError

    def check_end_work(self):
        if self.message.is_message_done():
            return True

        if self._check_done_of_concurrence_obj():
            try:
                result = self._get_result_of_concurrence_obj()
            except Exception as e:
                self.message.message_to_done(exception_info=str(e))
            else:
                self.message.message_to_done(result=result)
            return True

        if self.timeout and datetime.now() - self._time_start_work > self.timeout:
            self.stop_work()
            self.message.message_to_done(
                exception_info=f"the task was completed by server timeout {self.timeout.total_seconds()} secs.")
            return True

        return False

    def get_task(self):
        self.check_end_work()
        return self.message

    def get_result(self):
        if self.message.is_message_done():
            return self.message.result
        return None
