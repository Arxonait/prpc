import datetime
import json
import logging
import uuid
from abc import ABC, abstractmethod

import redis

from main.task import Task, TaskDone


class QueueWithFeedback(ABC):
    @abstractmethod
    def __init__(self, config_broker: dict, name_queue: str, expire_task_feedback: datetime.timedelta):
        raise NotImplementedError

    @abstractmethod
    def _create_queue(self):
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
    def search_task_in_feedback(self, task_id) -> None | TaskDone:
        raise NotImplementedError

    @abstractmethod
    def _restoring_processing_tasks(self):
        raise NotImplementedError


class QueueWithFeedbackRedis(QueueWithFeedback):
    def __init__(self, config_broker: dict,
                 name_queue: str,
                 expire_task_feedback: datetime.timedelta,
                 expire_task_process: datetime.timedelta):

        self.redis_client = redis.Redis(host=config_broker["host"], port=config_broker["port"], db=config_broker["db"])
        self.name_queue = name_queue
        self.expire_task_feedback = expire_task_feedback
        self.expire_task_process = expire_task_process

        self._create_queue()
        restore_tasks = self._restoring_processing_tasks()
        logging.debug(f"Востановлены незавершенные (process) задачи. кол-во задач {len(restore_tasks)}")

    def _create_queue(self):
        pass

    def add_task_in_queue(self, task: Task):
        self.redis_client.lpush(self.name_queue, task.model_dump_json())

    def __get_name_task_feedback(self, task_id: uuid.UUID):
        return f"{self.name_queue}:feedback:{task_id}"

    def __pattern_name_task_processing(self):
        return f"{self.name_queue}:in_process:"

    def __get_name_task_in_process(self, task_id):
        return f"{self.__pattern_name_task_processing()}{task_id}"

    def add_task_in_feedback(self, task: TaskDone):
        self.__delete_task_from_in_process(task)
        self.redis_client.set(self.__get_name_task_feedback(task.task_id), task.model_dump_json(), self.expire_task_feedback)

    def search_task_in_feedback(self, task_id: uuid.UUID):
        result = self.redis_client.get(self.__get_name_task_feedback(task_id))
        self.redis_client.delete(self.__get_name_task_feedback(task_id))
        if result is None:
            return

        return TaskDone(**json.loads(result))

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

    def _restoring_processing_tasks(self) -> list[bytes]:
        keys = self.redis_client.keys(f"{self.__pattern_name_task_processing()}*")
        results = []
        for key in keys:
            result = self.redis_client.get(key)
            self.redis_client.delete(key)
            results.append(result)

        if results:
            self.redis_client.rpush(self.name_queue, *results)

        return results


class QueueWithFeedbackFactory:
    queue_with_feedback: dict[str, QueueWithFeedback] = {
        'redis': QueueWithFeedbackRedis,
    }

    @classmethod
    def get_queue(cls,
                  type_broker: str,
                  config_broker,
                  name_queue: str,
                  expire_task_feedback=datetime.timedelta(hours=12),
                  expire_task_process=datetime.timedelta(hours=12)) -> QueueWithFeedback:

        queue_class = cls.queue_with_feedback.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.queue_with_feedback.keys()}")

        return queue_class(config_broker, name_queue, expire_task_feedback, expire_task_process)
