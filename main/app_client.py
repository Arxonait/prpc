import asyncio
import datetime
import logging
import time
import os
from dotenv import load_dotenv

from main.brokers_module import QueueFactory, AbstractQueueClient
from main.task import Task
from main.type_module import ManagerTypes


def get_function_server():
    return AwaitableTask('get_function_server', (), {})


class ClientBroker:
    __instance = None

    @classmethod
    def get_instance(cls):
        if cls.__instance is None:
            ClientBroker()
        return cls.__instance

    def __init__(self):
        if self.__instance is not None:
            raise Exception("singleton cannot be instantiated more then once")
        ClientBroker.__instance = self

        type_broker, config_broker, queue_name = self._parse_env()
        queue_class = QueueFactory().get_queue_class_sync_client(type_broker)
        self.queue: AbstractQueueClient = queue_class(config_broker, queue_name)
        self.queue.init()

    def _parse_env(self):
        load_dotenv()
        type_broker = os.getenv("PRPC_TYPE_BROKER")
        config_broker = os.getenv("PRPC_URL_BROKER")
        # connect_async = os.getenv("PRPC_CONNECT_ASYNC", False)
        queue_name = os.getenv("PRPC_QUEUE_NAME")

        if not (type_broker and config_broker and queue_name):
            logging.error(f"{type_broker=}, {config_broker=}, {queue_name=}")
            raise Exception("env PRPC_TYPE_BROKER, PRPC_URL_BROKER, PRPC_QUEUE_NAME must be installed")

        return type_broker, config_broker, queue_name


class AwaitableTask:
    __client_broker = ClientBroker()
    _manager_types = ManagerTypes()

    def __init__(self, func_name: str, args: tuple, kwargs: dict):
        self._task: Task = Task(func_name=func_name, func_args=args, func_kwargs=kwargs)
        self._start_task()

    def _start_task(self):
        self.__client_broker.queue.add_task_in_queue(self._task)

    def check_done_task(self):
        if self._task.is_task_done():
            return True

        task_done = self.__client_broker.queue.search_task_in_feedback(task_id=self.get_task_id())
        if task_done is None:
            return False

        self._task = task_done
        return True

    def _check_timeout(self, start_wait, timeout):
        if timeout is not None and datetime.datetime.now() - start_wait > timeout:
            assert not self._task.is_task_done(), "при wait timeout должна быть возможность повторного ожидания"
            raise Exception(f"the task was completed by wait timeout {timeout.total_seconds()} secs.")

    def sync_wait_result_task(self, timeout: datetime.timedelta | None = None):
        start_wait = datetime.datetime.now()
        while True:
            if self.check_done_task():
                return self.get_result()
            self._check_timeout(start_wait, timeout)
            time.sleep(1)

    async def async_wait_result_task(self, timeout: datetime.timedelta | None = None):
        start_wait = datetime.datetime.now()
        while True:
            if self.check_done_task():
                return self.get_result()
            self._check_timeout(start_wait, timeout)
            await asyncio.sleep(1)

    def get_result(self):
        if not self._task.is_task_done():
            raise Exception("task in process")

        if self._task.exception_info:
            raise Exception(self._task.exception_info)

        return self._task.result

    @property
    def task(self):
        return self._task

    @property
    def time_execution(self) -> datetime.timedelta | None:
        if not self._task.is_task_done():
            return None

        return self._task.date_done_task - self._task.date_create_task

    def get_task_id(self):
        return self._task.task_id
