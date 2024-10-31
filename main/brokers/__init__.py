from abc import ABC, abstractmethod

from main.prpcmessage import PRPCMessage


class AbstractBroker(ABC):
    _prefix_name_queue = "prpc"
    _prefix_name_queue_feedback = f"{_prefix_name_queue}_feedback"

    def __init__(self, broker_url: str, queue_name: str, *args, **kwargs):
        self.queue_name = queue_name
        self.broker_url = broker_url

    def get_queue_name(self):
        return f"{self._prefix_name_queue}_{self.queue_name}"

    def get_queue_feedback_name(self):
        return f"{self._prefix_name_queue_feedback}_{self.queue_name}"


class AdminBroker(AbstractBroker):

    @abstractmethod
    async def init(self, *args, **kwargs):
        raise NotImplementedError


class ServerBroker(AbstractBroker):

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def get_next_message_from_queue(self):
        raise NotImplementedError

    @abstractmethod
    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        raise NotImplementedError


class ClientBroker(AbstractBroker):
    @abstractmethod
    def add_message_in_queue(self, message: PRPCMessage):
        raise NotImplementedError

    @abstractmethod
    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        raise NotImplementedError
