from abc import ABC, abstractmethod

from main.task import Task


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

    @abstractmethod
    async def create_queues(self, *args, **kwargs):
        raise NotImplementedError


class ServerBroker(AbstractBroker):

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def get_next_task_from_queue(self):
        raise NotImplementedError

    @abstractmethod
    async def add_task_in_feedback_queue(self, task: Task):
        raise NotImplementedError


class ClientBroker(AbstractBroker):
    @abstractmethod
    def add_task_in_queue(self, task: Task):
        raise NotImplementedError

    @abstractmethod
    def search_task_in_feedback(self, task: Task) -> Task | None:
        raise NotImplementedError
