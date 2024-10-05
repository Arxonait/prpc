import datetime
import json
import uuid
from abc import ABC, abstractmethod

import redis

from main.task import Task


class QueueWithFeedback(ABC):
    @abstractmethod
    def __init__(self, config_broker: dict, name_queue: str, expire_task_feedback: datetime.timedelta):
        raise NotImplementedError

    @abstractmethod
    def create_queue(self):
        raise NotImplementedError

    @abstractmethod
    def add_task_in_queue(self, task: Task):
        raise NotImplementedError

    @abstractmethod
    def get_next_task_in_queue(self) -> Task | None:
        raise NotImplementedError

    @abstractmethod
    def _save_task_in_process(self, task: Task):
        raise NotImplementedError

    @abstractmethod
    def add_task_in_feedback(self, task: Task):
        raise NotImplementedError

    @abstractmethod
    def search_task_in_feedback(self, task_id):
        raise NotImplementedError


class Broker(ABC):
    # @abstractmethod
    # def listen_task_in_topic(self, name_topic: str):
    #     raise NotImplementedError

    @abstractmethod
    def insert_task_in_topic(self, task_data):
        raise NotImplementedError


class QueueWithFeedbackRedis(QueueWithFeedback):
    def __init__(self, config_broker: dict, name_queue: str,
                 expire_task_feedback: datetime.timedelta = datetime.timedelta(hours=12),
                 expire_task_process: datetime.timedelta = datetime.timedelta(hours=12)):
        self.redis_client = redis.Redis(host=config_broker["host"], port=config_broker["port"], db=config_broker["db"])
        self.name_queue = name_queue
        self.expire_task_feedback = expire_task_feedback
        self.expire_task_process = expire_task_process

    def create_queue(self):
        pass

    def add_task_in_queue(self, task: Task):
        self.redis_client.lpush(self.name_queue, task.model_dump_json())

    def __get_name_task_feedback(self, task_id: uuid.UUID):
        return f"{self.name_queue}:feedback:{task_id}"

    def __get_name_task_in_process(self, task_id):
        return f"{self.name_queue}:in_process:{task_id}"

    def add_task_in_feedback(self, task: Task):
        self.__delete_task_from_in_process(task)
        self.redis_client.set(self.__get_name_task_feedback(task.task_id), task.model_dump_json(), self.expire_task_feedback)

    def search_task_in_feedback(self, task_id: uuid.UUID):
        result = self.redis_client.get(self.__get_name_task_feedback(task_id))
        self.redis_client.delete(self.__get_name_task_feedback(task_id))
        if result is None:
            return

        return Task(**json.loads(result))

    def get_next_task_in_queue(self) -> Task | None:
        result = self.redis_client.rpop(self.name_queue)
        if result is None:
            return

        task_data = json.loads(result)
        task = Task(**task_data)

        self._save_task_in_process(task)

        return task

    def _save_task_in_process(self, task: Task):
        self.redis_client.set(self.__get_name_task_in_process(task.task_id), task.model_dump_json(), self.expire_task_process)

    def __delete_task_from_in_process(self, task: Task):
        self.redis_client.delete(self.__get_name_task_in_process(task.task_id))


class QueueWithFeedbackFactory:
    queue_with_feedback: dict[str, QueueWithFeedback] = {
        'redis': QueueWithFeedbackRedis,
    }

    @classmethod
    def get_queue(cls, type_broker: str, config_broker, name_queue: str = "queue_task"):
        queue_class = cls.queue_with_feedback.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.queue_with_feedback.keys()}")

        return queue_class(config_broker, name_queue)
