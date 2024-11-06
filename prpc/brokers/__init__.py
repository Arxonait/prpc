from abc import ABC, abstractmethod
from typing import Any

from prpc.prpcmessage import PRPCMessage


class AbstractQueue(ABC):
    _prefix_name_queue = "prpc"
    _prefix_name_queue_feedback = f"{_prefix_name_queue}_feedback"

    def __init__(self, queue_name: str):
        self.queue = f"{self._prefix_name_queue}_{queue_name}"
        self.queue_feedback = f"{self._prefix_name_queue_feedback}_{queue_name}"

    def __str__(self):
        return f"Queue --- main queue - `{self.queue}`, feedback queue - `{self.queue_feedback}`"

    @abstractmethod
    def convert_message_queue_to_prpc_message(self, message) -> PRPCMessage:
        raise NotImplementedError

    def serialize_message_for_feedback(self, message: PRPCMessage):
        return message.serialize()


class AbstractQueueRaw(AbstractQueue, ABC):

    def __init__(self, queue_name: str):
        self.queue = f"{self._prefix_name_queue}_raw_{queue_name}"
        self.queue_feedback = f"{self._prefix_name_queue_feedback}_raw_{queue_name}"

    def __str__(self):
        return f"Queue raw --- main queue - `{self.queue}`, feedback queue - `{self.queue_feedback}`"

    def serialize_message_for_feedback(self, message: PRPCMessage):
        return message.serialize(True)


class AbstractBroker(ABC):
    def __init__(self, broker_url: str, queue_name: str):
        self._queue_name = queue_name
        self.broker_url = broker_url

        self.queue = self._init_queue(queue_name)
        self.queue_raw = self._init_queue_raw(queue_name)
        self.queues = (self.queue, self.queue_raw)

    @abstractmethod
    def _init_queue(self, queue_name) -> AbstractQueue:
        raise NotImplementedError

    @abstractmethod
    def _init_queue_raw(self, queue_name) -> AbstractQueueRaw:
        raise NotImplementedError


class AdminBroker(AbstractBroker):

    def __init__(self, broker_url: str, queue_name: str, group_name=None):
        super().__init__(broker_url, queue_name)
        self._current_message: Any | None = None
        self._group_name: str | None = group_name

    @abstractmethod
    async def init(self, *args, **kwargs):
        raise NotImplementedError


class ServerBroker(AbstractBroker):
    _count_instance = 0

    def __init__(self, broker_url: str, queue_name: str, group_name):
        super().__init__(broker_url, queue_name)
        self._current_message: Any | None = None
        self._group_name = group_name
        ServerBroker._count_instance += 1

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def get_next_message_from_queue(self):
        raise NotImplementedError

    @abstractmethod
    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        raise NotImplementedError

    def _get_queue_for_message_queue_name(self, queue_name: str) -> AbstractQueue:
        queues = list(filter(lambda queue: queue_name == queue.queue, self.queues))
        if len(queues) != 1:
            raise Exception(f"Не нашлась очередь `{queue_name}` из доступных очередей {[queue.queue for queue in (self.queue, self.queue_raw)]}")

        queue = queues[0]
        return queue

    def _set_current_message(self, message):
        self._current_message = message

    def _get_current_message(self):
        assert self._current_message is not None, "`_current_message_stream mustn't be None`"
        value = self._current_message
        self._current_message_stream = None
        return value


class ClientBroker(AbstractBroker):
    @abstractmethod
    def add_message_in_queue(self, message: PRPCMessage):
        raise NotImplementedError

    @abstractmethod
    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

