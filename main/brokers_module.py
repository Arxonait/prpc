import asyncio
import datetime
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from typing import Literal

import redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka import KafkaProducer, KafkaConsumer

from main.task import Task


BROKER_ANNOTATION = Literal["redis", "kafka"]


class AbstractQueue(ABC):
    def __init__(self, confing_broker: dict | str, queue_name: str):
        self.config_broker = confing_broker
        self._queue_name = queue_name
        self.client = None

    def init(self):
        self._create_client()

    @abstractmethod
    async def _create_client(self):
        raise NotImplementedError

    @abstractmethod
    def _pattern_queue_feedback(self):
        raise NotImplementedError

    @abstractmethod
    def _pattern_queue(self):
        raise NotImplementedError


class AbstractQueueClient(AbstractQueue, ABC):

    @abstractmethod
    def add_task_in_queue(self, task: Task):
        raise NotImplementedError

    @abstractmethod
    def search_task_in_feedback(self, task_id: uuid.UUID) -> Task:
        raise NotImplementedError


class AbstractQueueServer(AbstractQueue, ABC):
    def __init__(self, confing_broker: dict | str, queue_name: str,
                 expire_task_feedback: datetime.timedelta = datetime.timedelta(hours=2),
                 expire_task_process: datetime.timedelta = datetime.timedelta(hours=2)):
        super().__init__(confing_broker, queue_name)
        self._expire_task_feedback = expire_task_feedback
        self._expire_task_process = expire_task_process

    @abstractmethod
    async def _create_queues(self):
        raise NotImplementedError

    @abstractmethod
    async def _restoring_processing_tasks(self) -> list[bytes]:
        raise NotImplementedError

    @abstractmethod
    async def get_next_task_from_queue(self):
        raise NotImplementedError

    @abstractmethod
    async def add_task_in_feedback_queue(self, task: Task):
        raise NotImplementedError


class AbstractQueueRedis(AbstractQueue, ABC):
    def _pattern_queue_feedback(self):
        return f"prpc:feedback:{self._queue_name}"

    def _pattern_queue(self):
        return f"prpc:{self._queue_name}"

    def _pattern_queue_feedback_task_id(self, task_id):
        return f"{self._pattern_queue_feedback()}:{task_id}"


class ClientQueueRedisSync(AbstractQueueClient, AbstractQueueRedis):

    def init(self):
        self._create_client()

    def _create_client(self):
        if isinstance(self.config_broker, str):
            self.client = redis.from_url(self.config_broker)
        else:
            self.client = redis.Redis(host=self.config_broker["host"], port=self.config_broker["port"],
                                      db=self.config_broker["db"])

    def add_task_in_queue(self, task: Task):
        self.client.lpush(self._pattern_queue(), task.serialize())

    def search_task_in_feedback(self, task_id: uuid.UUID):
        result = self.client.get(self._pattern_queue_feedback_task_id(task_id))
        self.client.delete(self._pattern_queue_feedback_task_id(task_id))
        if result is None:
            return

        return Task.deserialize(result)


# class ClientQueueRedisAsync(AbstractQueueClient, AbstractQueueRedis):
#
#     def init(self):
#         self._create_client()
#
#     async def _create_client(self):
#         if isinstance(self.config_broker, str):
#             self.client = await redis.asyncio.from_url(self.config_broker)
#         else:
#             self.client = await redis.asyncio.Redis(host=self.config_broker["host"],
#                                                     port=self.config_broker["port"],
#                                                     db=self.config_broker["db"])
#
#     async def add_task_in_queue(self, task: Task):
#         await self.client.lpush(self._pattern_queue(), task.model_dump_json())
#
#     async def search_task_in_feedback(self, task_id: uuid.UUID):
#         result = await self.client.get(self._pattern_queue_feedback_task_id(task_id))
#         await self.client.delete(self._pattern_queue_feedback_task_id(task_id))
#         if result is None:
#             return
#
#         return TaskDone(**json.loads(result))


class ServerQueueRedis(AbstractQueueRedis, AbstractQueueServer):

    async def init(self):
        await self._create_client()
        await self._create_queues()
        restore_tasks = await self._restoring_processing_tasks()
        logging.debug(f"Востановлены незавершенные (process) задачи. кол-во задач {len(restore_tasks)}")

    def _pattern_name_queue_in_process_task(self, task_id: uuid.UUID):
        return f"{self._pattern_name_queue_in_process()}:{task_id}"

    def _pattern_name_queue_in_process(self):
        return f"prpc:in_process:{self._queue_name}"

    async def _create_client(self):
        if isinstance(self.config_broker, str):
            self.client = await redis.asyncio.from_url(self.config_broker)
        else:
            self.client = await redis.asyncio.Redis(host=self.config_broker["host"],
                                                    port=self.config_broker["port"],
                                                    db=self.config_broker["db"])

    async def _create_queues(self):
        pass

    async def get_next_task_from_queue(self):
        key, value = await self.client.brpop(self._pattern_queue())
        task = Task.deserialize(value)

        await self._save_task_in_process(task)
        return task

    async def add_task_in_feedback_queue(self, task: Task):
        await self._delete_task_in_process(task)
        await self.client.set(self._pattern_queue_feedback_task_id(task.task_id), task.serialize(),
                              self._expire_task_feedback)

    async def _save_task_in_process(self, task: Task):
        await self.client.set(self._pattern_name_queue_in_process_task(task.task_id), task.serialize(),
                              self._expire_task_process)

    async def _delete_task_in_process(self, task: Task):
        await self.client.delete(self._pattern_name_queue_in_process_task(task.task_id))

    async def _restoring_processing_tasks(self) -> list[bytes]:
        keys = await self.client.keys(f"{self._pattern_name_queue_in_process()}:*")
        results = []
        for key in keys:
            result = await self.client.get(key)
            await self.client.delete(key)
            results.append(result)

        if results:
            await self.client.rpush(self._pattern_queue(), *results)

        return results


class AbstractQueueKafka(AbstractQueue, ABC):
    def _pattern_queue_feedback(self):
        return f"prpc_feedback_{self._queue_name}"

    def _pattern_queue(self):
        return f"prpc_{self._queue_name}"


class ServerQueueKafka(AbstractQueueKafka, AbstractQueueServer):

    def __init__(self, config_broker, queue_name):
        super().__init__(config_broker, queue_name)
        self.consumer = AIOKafkaConsumer(
            self._pattern_queue(),
            bootstrap_servers=self.config_broker,
            group_id="prpc_group", # todo expire
            auto_offset_reset='earliest'
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.config_broker)

    async def init(self):
        await self._create_client()
        await self._create_queues()

    async def _create_client(self):
        await self.consumer.start()
        await self.producer.start()

    async def _create_queues(self):
        pass

    async def get_next_task_from_queue(self):
        try:
            msg = await self.consumer.getone()
            task = Task.deserialize(msg.value)
            return task
        finally:
            await self.consumer.stop()

    async def add_task_in_feedback_queue(self, task: Task):
        # todo update offset
        await self.producer.send_and_wait(self._pattern_queue_feedback(), task.serialize().encode())

    async def _restoring_processing_tasks(self) -> list[bytes]:
        pass


class ClientQueueKafkaSync(AbstractQueueClient, AbstractQueueKafka):

    def __init__(self, config_broker, queue_name):
        super().__init__(config_broker, queue_name)
        self.consumer = KafkaConsumer(
            self._pattern_queue_feedback()

        )
        self.consumer.offsets_for_times()
        self.consumer.seek()
        self.consumer = AIOKafkaConsumer(
            self._pattern_queue_feedback(),
            bootstrap_servers=self.config_broker,
            group_id="prpc_group", # убрать ли
            auto_offset_reset='earliest' # что это
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.config_broker)

    def init(self):
        self._create_client()

    def _create_client(self):
        if isinstance(self.config_broker, str):
            self.client = redis.from_url(self.config_broker)
        else:
            self.client = redis.Redis(host=self.config_broker["host"], port=self.config_broker["port"],
                                      db=self.config_broker["db"])

    def add_task_in_queue(self, task: Task):
        self.client.lpush(self._pattern_queue(), task.serialize())

    def search_task_in_feedback(self, task_id: uuid.UUID):
        result = self.client.get(self._pattern_queue_feedback_task_id(task_id))
        self.client.delete(self._pattern_queue_feedback_task_id(task_id))
        if result is None:
            return

        return Task.deserialize(result)


class QueueFactory:
    server_queue: dict[str, AbstractQueueServer] = {
        'redis': ServerQueueRedis,
        'kafka': ServerQueueKafka
    }

    sync_client_queue: dict[str, AbstractQueueClient] = {
        'redis': ClientQueueRedisSync,
    }

    # async_client_queue: dict[str, AbstractQueueClient] = {
    #     'redis': ClientQueueRedisAsync,
    # }

    @classmethod
    def get_queue_class_server(cls, type_broker) -> AbstractQueueServer:
        queue_class = cls.server_queue.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.server_queue.keys()}")

        return queue_class

    @classmethod
    def get_queue_class_sync_client(cls, type_broker: Literal["redis"]) -> AbstractQueueClient:
        queue_class = cls.sync_client_queue.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.sync_client_queue.keys()}")

        return queue_class

    # @classmethod
    # def get_queue_class_async_client(cls, type_broker: Literal["redis"]) -> AbstractQueueClient:
    #     queue_class = cls.async_client_queue.get(type_broker)
    #
    #     if queue_class is None:
    #         raise Exception(f"only this broker: {cls.async_client_queue.keys()}")
    #
    #     return queue_class
