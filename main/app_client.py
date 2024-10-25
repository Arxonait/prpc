import asyncio
import datetime
import logging
import time
import os
from dotenv import load_dotenv

from main.brokers.brokers_factory import BrokerFactory
from main.prpcmessage import PRPCMessage
from main.brokers import ClientBroker


def get_function_server():
    return AwaitableTask('get_function_server', (), {})


def ping():
    return AwaitableTask('ping', (), {})


class ManagerClientBroker:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            ManagerClientBroker()
        return cls._instance

    def __init__(self):
        if self._instance is not None:
            raise Exception("singleton cannot be instantiated more then once")
        ManagerClientBroker.__instance = self

        type_broker, config_broker, queue_name = self._get_init_data()
        queue_class = BrokerFactory().get_broker_class_sync_client(type_broker)
        self.queue: ClientBroker = queue_class(config_broker, queue_name) # todo при нескольких ожиданий задач (в потоках, в процесах) возможны ошибки

    def _get_init_data(self):

        env_data = self._get_env()
        if all(item is None for item in env_data):
            load_dotenv()
            env_data = self._get_env()

        type_broker, config_broker, queue_name = env_data

        if not (type_broker and config_broker and queue_name):
            logging.error(f"{type_broker=}, {config_broker=}, {queue_name=}")
            raise Exception("env PRPC_TYPE_BROKER, PRPC_URL_BROKER, PRPC_QUEUE_NAME must be installed")
        return type_broker, config_broker, queue_name

    def _get_env(self):
        type_broker = os.getenv("PRPC_TYPE_BROKER")
        config_broker = os.getenv("PRPC_URL_BROKER")
        # connect_async = os.getenv("PRPC_CONNECT_ASYNC", False)
        queue_name = os.getenv("PRPC_QUEUE_NAME")
        return type_broker, config_broker, queue_name


class AwaitableTask:
    _client_broker: ManagerClientBroker | None = None

    def __init__(self, func_name: str, args: tuple, kwargs: dict):
        if self._client_broker is None:
            self.__client_broker = ManagerClientBroker()
        self._message: PRPCMessage = PRPCMessage(func_name=func_name, func_args=args, func_kwargs=kwargs)
        self._send_message()

    def _send_message(self):
        self.__client_broker.queue.add_message_in_queue(self._message)

    def check_done_task(self):
        if self._message.is_message_done():
            return True

        message_done = self.__client_broker.queue.search_message_in_feedback(self._message)
        if message_done is None:
            return False

        self._message = message_done
        return True

    def _check_timeout(self, start_wait, timeout):
        if timeout is not None and datetime.datetime.now() - start_wait > timeout:
            assert not self._message.is_message_done(), "при wait timeout должна быть возможность повторного ожидания"
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
        if not self._message.is_message_done():
            raise Exception("task in process")

        if self._message.exception_info:
            raise Exception(self._message.exception_info)

        return self._message.result

    @property
    def message(self):
        return self._message

    @property
    def time_execution(self) -> datetime.timedelta | None:
        if not self._message.is_message_done():
            return None

        return self._message.date_done_message - self._message.date_create_message
