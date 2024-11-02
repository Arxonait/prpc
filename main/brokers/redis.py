import datetime
from abc import ABC, abstractmethod
from typing import Any

import redis
from redis import ResponseError

from main.brokers import AbstractBroker, AdminBroker, ServerBroker, ClientBroker, AbstractQueue, AbstractQueueRaw
from main.brokers.redis_support_code import StreamsData, MessageFromSteam
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

    def _get_client(self):
        assert self._client is not None
        return self._client

    @abstractmethod
    def _create_client(self):
        raise NotImplementedError

    def _init_queue(self, queue_name):
        return RedisQueue(queue_name)

    def _init_queue_raw(self, queue_name):
        return RedisRawQueue(queue_name)


class RedisAdminBroker(AdminBroker, AbstractRedisBroker):

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    async def init(self, *args, **kwargs):
        await self._create_client()
        await self._create_groups()

    async def _create_groups(self):
        await self._create_group_stream(self.queue.queue)
        await self._create_group_stream(self.queue_raw.queue)

    async def _create_group_stream(self, stream):
        try:
            await self._client.xgroup_create(stream, self._group_name, id="$", mkstream=True)
            logger.info(f"Группа {self._group_name}` создана для stream `{stream}`")
        except ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.debug(f"Группа уже создана для stream `{stream}` --- имя группы `{self._group_name}`")
            else:
                raise e


class RedisServerBroker(AbstractRedisBroker, ServerBroker):

    def __init__(self, *args, context, **kwargs):
        super().__init__(*args, **kwargs)
        self._expire_message_feedback: datetime.timedelta = Settings.redis_expire_task_feedback()
        self._heartbeat_interval = Settings.redis_heartbeat()
        self._recover_interval = Settings.redis_recover_interval()

        self._current_message_stream: MessageFromSteam | None = None
        self._buffer_messages: list[MessageFromSteam] = []

        self._queue_number = context["queue_number"]
        self._instance_prpc_name = Settings.instance_name()

    async def init(self):
        await self._create_client()

    async def _create_client(self):
        self._client = await redis.asyncio.from_url(self.broker_url)

    def _set_current_message_stream(self, message):
        self._current_message_stream = message

    def _get_current_message_stream(self):
        assert self._current_message_stream is not None, "`_current_message_stream mustn't be None`"
        value = self._current_message_stream
        self._current_message_stream = None
        return value

    def _get_consumer_name(self):
        return f"{self._instance_prpc_name}_{self._queue_number}"

    async def _get_message_from_buffer(self):
        if len(self._buffer_messages) == 0:

            stream_data = await self._get_client().xreadgroup(self._group_name,
                                                              self._get_consumer_name(),
                                                              {queue.queue: ">" for queue in self.queues},
                                                              block=0, count=1)
            stream_data = StreamsData(stream_data)
            for message in stream_data.messages:
                message.start_send_heartbeat(self._get_client(), self._heartbeat_interval, self._group_name,
                                             self._get_consumer_name())

            self._buffer_messages.extend(stream_data.messages)

        return self._buffer_messages.pop()

    async def _recover_pending_messages(self, stream_name) -> MessageFromSteam | None:
        pending_messages: list[dict] = await self._get_client().xpending_range(stream_name, self._group_name, "-", "+",
                                                                               count=1000)
        if len(pending_messages) == 0:
            return None

        for pending_message in pending_messages:
            message_id = pending_message["message_id"]
            messages_data: list[list[Any, dict]] = await self._get_client().xclaim(stream_name,
                                                                                   self._group_name,
                                                                                   self._get_consumer_name(),
                                                                                   min_idle_time=self._recover_interval * 1000,
                                                                                   message_ids=[message_id])
            if messages_data:
                message = MessageFromSteam(stream_name, messages_data[0])
                logger.debug(f"Redis - востановленно сообщение {message_id} из pending")
                message.start_send_heartbeat(self._get_client(), self._heartbeat_interval, self._group_name,
                                             self._get_consumer_name())
                return message

    async def _recover_pending_messages_streams(self) -> MessageFromSteam | None:
        for queue in self.queues:
            stream_name = queue.queue
            message = await self._recover_pending_messages(stream_name)
            if message is not None:
                break

        return message

    async def get_next_message_from_queue(self):
        message: MessageFromSteam = None
        if len(self._buffer_messages) == 0:
            message = await self._recover_pending_messages_streams()

        if message is None:
            message = await self._get_message_from_buffer()

        self._set_current_message_stream(message)
        queue = self._get_queue_for_message_stream(message)
        prpc_message = queue.convert_message_stream_to_prpc_message(message)
        return prpc_message

    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        message_stream = self._get_current_message_stream()
        queue = self._get_queue_for_message_stream(message_stream)

        await self._get_client().set(queue.get_queue_feedback_id(message.message_id), queue.serialize_message_for_feedback(message),
                                     self._expire_message_feedback)
        await message_stream.remove_message_from_pending(self._get_client(), self._group_name)

    def _get_queue_for_message_stream(self, message_stream: MessageFromSteam):
        queues = list(filter(lambda queue: message_stream.stream == queue.queue, self.queues))
        if len(queues) != 1:
            raise Exception(f"Не нашлась очередь `{message_stream.stream}` из доступных очередей {[queue.queue for queue in (self.queue, self.queue_raw)]}")

        queue: RedisQueue = queues[0]
        return queue


class RedisClientBroker(AbstractRedisBroker, ClientBroker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_client()

    def _create_client(self):
        self._client = redis.from_url(self.broker_url)

    def add_message_in_queue(self, message: PRPCMessage):
        self._client.xadd(self.queue.queue, self.queue.convert_prpc_message_to_message_stream(message))

    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        queue: RedisQueue = self.queue
        result = self._client.get(queue.get_queue_feedback_id(message.message_id))
        self._client.delete(queue.get_queue_feedback_id(message.message_id))
        if result is None:
            return

        return PRPCMessage.deserialize(result)


class RedisQueue(AbstractQueue):
    _key_data = "message"

    def convert_prpc_message_to_message_stream(self, message: PRPCMessage):
        return {self._key_data: message.serialize()}

    def convert_message_stream_to_prpc_message(self, message: MessageFromSteam):
        return PRPCMessage.deserialize(message.data[self._key_data.encode()])

    def serialize_message_for_feedback(self, message: PRPCMessage):
        return message.serialize()

    def get_queue_feedback_id(self, message_id):
        return f"{self.queue_feedback}_{message_id}"


class RedisRawQueue(AbstractQueueRaw, RedisQueue):
    def __init__(self, queue_name: str):
        AbstractQueueRaw.__init__(self, queue_name)

    def convert_message_stream_to_prpc_message(self, message: MessageFromSteam):
        return PRPCMessage.deserialize_raw(message.data[self._key_data.encode()])

    def serialize_message_for_feedback(self, message: PRPCMessage):
        return message.serialize(True)

