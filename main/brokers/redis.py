import asyncio
import datetime
from abc import ABC, abstractmethod
from asyncio import Task
from typing import Literal, Any

import redis
from redis import ResponseError

from main.brokers import AbstractBroker, AdminBroker, ServerBroker, ClientBroker
from main.prpcmessage import PRPCMessage
from main.settings_server import Settings
from main.support_module.loggs import Logger

logger = Logger.get_instance()
logger = logger.prpc_logger


class AbstractRedisBroker(AbstractBroker, ABC):
    _group_name = Settings.group_name()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: redis.asyncio.Redis | None = None

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
        await self._create_groups()

    async def _create_groups(self):
        try:
            await self._client.xgroup_create(self.get_queue_name(), self._group_name, id="$", mkstream=True)
            logger.info(f"group {self._group_name}` is created")
        except ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.debug(f"the group has already been created --- name group `{self._group_name}`")
            else:
                raise e


class RedisServerBroker(AbstractRedisBroker, ServerBroker):

    def __init__(self, *args, context, **kwargs):
        super().__init__(*args, **kwargs)
        self._expire_message_feedback: datetime.timedelta = Settings.redis_expire_task_feedback()
        self._heartbeat_interval = Settings.redis_heartbeat()
        self._recover_interval = Settings.redis_recover_interval()

        self._current_message_stream: tuple[dict, Task] | None = None
        self._buffer_messages: list[tuple[dict, Task]] = []

        self._queue_number = context["queue_number"]
        self._instance_prpc_name = Settings.instance_name()

        self._streams = (self.get_queue_name(),)

    async def init(self):
        await self._create_client()

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    def _set_current_message_stream(self, message):
        self._current_message_stream = message

    def _get_current_message_stream(self):
        assert self._current_message_stream is not None  # todo log.debug message
        value = self._current_message_stream
        self._current_message_stream = None
        return value

    def parse_stream_data(self, stream_data) -> list[dict[Literal["stream", "id", "data"], Any]]:
        parsed_data = []
        for stream_name, messages in stream_data:
            for message_id, message in messages:
                parsed_data.append({
                    "stream": stream_name,
                    "id": message_id,
                    "data": message
                })
        return parsed_data

    def _get_consumer_name(self):
        return f"{self._instance_prpc_name}_{self._queue_number}"

    async def _send_heartbeat(self, message_id, stream_name):
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            await self._get_client().xclaim(
                stream_name,
                self._group_name,
                self._get_consumer_name(),
                min_idle_time=self._heartbeat_interval * 1000,
                message_ids=[message_id]
            )
            logger.debug(f"Redis - отправлен heartbeat для сообщения {message_id}")

    async def _get_message_from_buffer(self):
        if len(self._buffer_messages) == 0:

            stream_data = await self._get_client().xreadgroup(self._group_name,
                                                              self._get_consumer_name(),
                                                              {stream: ">" for stream in self._streams},
                                                              block=0, count=1)
            parsed_stream_data = self.parse_stream_data(stream_data)
            heartbeats = []
            for message in parsed_stream_data:
                task = asyncio.create_task(self._send_heartbeat(message["id"], message["stream"]))
                heartbeats.append(task)
            self._buffer_messages.extend(list(zip(parsed_stream_data, heartbeats)))

        return self._buffer_messages.pop()

    async def _recover_pending_messages(self, stream_name) -> tuple[dict, Task] | None:
        pending_messages: list[dict] = await self._get_client().xpending_range(stream_name, self._group_name, "-", "+",
                                                                               count=1000)
        if len(pending_messages) == 0:
            return None

        for pending_message in pending_messages:
            message_id = pending_message["message_id"]
            message_data: list[Any, dict] = await self._get_client().xclaim(stream_name,
                                                                            self._group_name,
                                                                            self._get_consumer_name(),
                                                                            min_idle_time=self._recover_interval * 1000,
                                                                            message_ids=[message_id])
            stream_data = [(stream_name.encode(), message_data), ]  # подготовка данных для парсинга
            parsed_stream_data = self.parse_stream_data(stream_data)
            if parsed_stream_data:
                logger.debug(f"Redis - востановленно сообщение {message_id} из pending")
                message = parsed_stream_data[0]
                task = asyncio.create_task(self._send_heartbeat(message["id"], message["stream"]))
                return message, task

    async def _recover_pending_messages_streams(self) -> tuple[dict, Task] | None:
        for stream_name in self._streams:
            message = await self._recover_pending_messages(stream_name)
            if message is not None:
                break

        return message

    async def get_next_message_from_queue(self):
        message = None
        if len(self._buffer_messages) == 0:
            message = await self._recover_pending_messages_streams()

        if message is None:
            message = await self._get_message_from_buffer()

        if message[0]["stream"].decode("utf8") == self.get_queue_name():
            self._set_current_message_stream(message)
            prpc_message = PRPCMessage.deserialize(message[0]["data"]["serialized_data".encode()])
        else:
            # todo
            raise Exception("uncorect name stream")  # todo Exception text
        return prpc_message

    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        message_stream = self._get_current_message_stream()

        await self._get_client().set(self.get_queue_feedback_name_message_id(message.message_id), message.serialize(),
                                     self._expire_message_feedback)
        await self._get_client().xack(message_stream[0]["stream"], self._group_name, message_stream[0]["id"])
        message_stream[1].cancel()


class RedisClientBroker(AbstractRedisBroker, ClientBroker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_client()

    def _create_client(self):
        self._client = redis.from_url(self.broker_url)

    def add_message_in_queue(self, message: PRPCMessage):
        self._client.xadd(self.get_queue_name(), {"serialized_data": message.serialize()})

    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        result = self._client.get(self.get_queue_feedback_name_message_id(message.message_id))
        self._client.delete(self.get_queue_feedback_name_message_id(message.message_id))
        if result is None:
            return

        return PRPCMessage.deserialize(result)
