import datetime
import logging
from abc import ABC, abstractmethod

import redis

from main import loggs
from main.brokers import ServerBroker, ClientBroker, AbstractBroker, AdminBroker
from main.loggs import Logger
from main.settings_server import Settings
from main.prpcmessage import PRPCMessage

logger = Logger.get_instance()
logger = logger.prpc_logger


class AbstractRedisBroker(AbstractBroker, ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: redis.asyncio.Redis | None = None

    def get_queue_process_name(self):
        return f"{self.get_queue_name()}_process"

    def get_queue_process_name_message_id(self, message_id):
        return f"{self.get_queue_process_name()}_{message_id}"

    def get_queue_feedback_name_message_id(self, message_id):
        return f"{self.get_queue_feedback_name()}_{message_id}"

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
        restore_messages = await self._restoring_processing_messages()
        logger.info(f"Redis: востановлены незавершенные задачи (сообщения). Кол-во сообщений {len(restore_messages)}")

    async def create_queues(self, *args, **kwargs):
        return

    async def _restoring_processing_messages(self) -> list[bytes]:
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
        self._expire_message_feedback: datetime.timedelta = Settings.redis_expire_task_feedback()
        self._expire_message_process: datetime.timedelta = Settings.redis_expire_task_process()

    async def init(self):
        await self._create_client()

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    async def get_next_message_from_queue(self):
        key, value = await self._get_client().brpop(self.get_queue_name())
        message = PRPCMessage.deserialize(value)

        await self._save_message_in_process(message)
        return message

    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        await self._delete_message_in_process(message)
        await self._get_client().set(self.get_queue_feedback_name_message_id(message.message_id), message.serialize(),
                                     self._expire_message_feedback)

    async def _save_message_in_process(self, message: PRPCMessage):
        await self._get_client().set(self.get_queue_process_name_message_id(message.message_id), message.serialize(),
                                     self._expire_message_process)

    async def _delete_message_in_process(self, message: PRPCMessage):
        await self._get_client().delete(self.get_queue_process_name_message_id(message.message_id))


class RedisClientBroker(AbstractRedisBroker, ClientBroker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_client()

    def _create_client(self):
        self._client = redis.from_url(self.broker_url)

    def add_message_in_queue(self, message: PRPCMessage):
        self._client.lpush(self.get_queue_name(), message.serialize())

    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        result = self._client.get(self.get_queue_feedback_name_message_id(message.message_id))
        self._client.delete(self.get_queue_feedback_name_message_id(message.message_id))
        if result is None:
            return

        return PRPCMessage.deserialize(result)
