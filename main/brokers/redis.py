import datetime
import logging
from abc import ABC, abstractmethod

import redis

from main.brokers import ServerBroker, ClientBroker, AbstractBroker, AdminBroker
from main.task import Task

_REDIS_CONFIG = {
    "expire_task_feedback": datetime.timedelta(hours=6),
    "expire_task_process": datetime.timedelta(days=5)
} # todo parse env


class AbstractRedisBroker(AbstractBroker, ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: redis.asyncio.Redis | None = None

    def get_queue_process_name(self):
        return f"{self.get_queue_name()}_process"

    def get_queue_process_name_task_id(self, task_id):
        return f"{self.get_queue_process_name()}_{task_id}"

    def get_queue_feedback_name_task_id(self, task_id):
        return f"{self.get_queue_feedback_name()}_{task_id}"

    def _get_client(self):
        assert self._client is not None
        return self._client

    @abstractmethod
    def _create_client(self):
        raise NotImplementedError


class RedisAdminBroker(AdminBroker, AbstractRedisBroker):

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    async def init(self, *args, **kwargs):
        await self._create_client()
        await self.create_queues()
        restore_tasks = await self._restoring_processing_tasks()
        logging.info(f"Redis: востановлены незавершенные задачи. Кол-во задач {len(restore_tasks)}")

    async def create_queues(self, *args, **kwargs):
        return

    async def _restoring_processing_tasks(self) -> list[bytes]:
        keys = await self._get_client().keys(f"{self.get_queue_process_name()}*")
        results = []
        for key in keys:
            result = await self._get_client().get(key)
            await self._get_client().delete(key)
            results.append(result)

        if results:
            await self._get_client().rpush(self.get_queue_name(), *results)

        return results


class RedisServerBroker(AbstractRedisBroker, ServerBroker):
    # todo to streams

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._expire_task_feedback: datetime.timedelta = _REDIS_CONFIG["expire_task_feedback"]
        self._expire_task_process: datetime.timedelta = _REDIS_CONFIG["expire_task_process"]

    async def init(self):
        await self._create_client()

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    async def get_next_task_from_queue(self):
        key, value = await self._get_client().brpop(self.get_queue_name())
        task = Task.deserialize(value)

        await self._save_task_in_process(task)
        return task

    async def add_task_in_feedback_queue(self, task: Task):
        await self._delete_task_in_process(task)
        await self._get_client().set(self.get_queue_feedback_name_task_id(task.task_id), task.serialize(),
                                     self._expire_task_feedback)

    async def _save_task_in_process(self, task: Task):
        await self._get_client().set(self.get_queue_process_name_task_id(task.task_id), task.serialize(),
                                     self._expire_task_process)

    async def _delete_task_in_process(self, task: Task):
        await self._get_client().delete(self.get_queue_process_name_task_id(task.task_id))


class RedisClientBroker(AbstractRedisBroker, ClientBroker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_client()

    def _create_client(self):
        self._client = redis.from_url(self.broker_url)

    def add_task_in_queue(self, task: Task):
        self._client.lpush(self.get_queue_name(), task.serialize())

    def search_task_in_feedback(self, task: Task) -> Task | None:
        result = self._client.get(self.get_queue_feedback_name_task_id(task.task_id))
        self._client.delete(self.get_queue_feedback_name_task_id(task.task_id))
        if result is None:
            return

        return Task.deserialize(result)
