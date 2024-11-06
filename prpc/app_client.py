import asyncio
import datetime
import logging
import time
import os
from dotenv import load_dotenv

from prpc.brokers.brokers_factory import BrokerFactory
from prpc.prpcmessage import PRPCMessage
from prpc.brokers import ClientBroker
from prpc.support_module.exceptions import ClientTimeOutError


def get_function_server():
    return AwaitableTask('get_function_server', (), {})


def ping():
    return AwaitableTask('ping', (), {})


class ClientForSendMessage:
    _instance = None

    client_broker_class = None
    url_broker = None
    queue_name = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            ClientForSendMessage()
        return cls._instance

    def __init__(self):
        if self._instance is not None:
            raise Exception("singleton cannot be instantiated more then once")

        ClientForSendMessage._instance = self

        type_broker = os.getenv("PRPC_TYPE_BROKER")
        self.url_broker = os.getenv("PRPC_URL_BROKER")
        self.queue_name = os.getenv("PRPC_QUEUE_NAME")
        assert type_broker and self.url_broker and self.queue_name, "env PRPC_TYPE_BROKER, PRPC_URL_BROKER, PRPC_QUEUE_NAME must be installed"
        self.client_broker_class = BrokerFactory().get_broker_class_sync_client(type_broker)

        self._client_broker: ClientBroker = self.client_broker_class(self.url_broker, self.queue_name)

    def send_message(self, prpc_message):
        self._client_broker.add_message_in_queue(prpc_message)


class AwaitableTask:
    _client_for_send_message: ClientForSendMessage | None = None

    def __init__(self, func_name: str, args: tuple, kwargs: dict):
        if self._client_for_send_message is None:
            AwaitableTask._client_for_send_message = ClientForSendMessage.get_instance()

        self._message: PRPCMessage = PRPCMessage(func_name=func_name, func_args=args, func_kwargs=kwargs)
        self._client_for_send_message.send_message(self._message)

        self._client_for_get_result: ClientBroker | None = None

    def _init_client_for_get_result(self):
        if self._client_for_get_result is None:
            client_broker_class = self._client_for_send_message.client_broker_class
            self._client_for_get_result = client_broker_class(self._client_for_send_message.url_broker,
                                                              self._client_for_send_message.queue_name)

    def _update_status_task(self):
        assert not self.is_done_task(), "task is done and _client_for_get_result is closed"
        self._init_client_for_get_result()

        message_done = self._client_for_get_result.search_message_in_feedback(self._message)
        if message_done is not None:
            self._message = message_done
            self._client_for_get_result.close()

    def is_done_task(self, *, update_status: bool = False):
        if self._message.is_message_done():
            return True
        if update_status:
            self._update_status_task()
            return self._message.is_message_done()
        return False

    def _check_timeout(self, start_wait, timeout):
        if timeout is not None and datetime.datetime.now() - start_wait > timeout:
            assert not self._message.is_message_done(), "при wait timeout должна быть возможность повторного ожидания"
            raise ClientTimeOutError(timeout)

    def sync_wait_result_task(self, timeout: datetime.timedelta | None = None):
        start_wait = datetime.datetime.now()
        while not self.is_done_task():
            self._update_status_task()
            if self.is_done_task():
                return self.get_result()
            self._check_timeout(start_wait, timeout)
            time.sleep(1)

    async def async_wait_result_task(self, timeout: datetime.timedelta | None = None):
        start_wait = datetime.datetime.now()
        while not self.is_done_task():
            self._update_status_task()
            if self.is_done_task():
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
