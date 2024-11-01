import asyncio
from asyncio import Task
from typing import Any

from main.support_module.loggs import Logger

logger = Logger.get_instance()
logger = logger.prpc_logger


class StreamsData:
    def __init__(self, stream_data):
        self.messages: list[MessageFromSteam] = []
        for stream_name, messages in stream_data:
            for message_id, message in messages:
                self.messages.append(MessageFromSteam(stream_name, [message_id, message]))


class MessageFromSteam:

    def __init__(self, stream: str | bytes, message_data: list[Any, dict]):
        self.task_heartbeat: Task | None = None
        self.stream = stream if isinstance(stream, str) else stream.decode("utf8")
        self.message_id = message_data[0]
        self.data = message_data[1]

    async def _task_send_heartbeat(self, client_redis, heartbeat_interval: int, group_name, consumer_name):
        while True:
            await asyncio.sleep(heartbeat_interval)
            await client_redis.xclaim(
                self.stream,
                group_name,
                consumer_name,
                min_idle_time=heartbeat_interval * 1000,
                message_ids=[self.message_id]
            )
            logger.debug(f"Redis - отправлен heartbeat для сообщения {self.message_id}")

    def start_send_heartbeat(self, client_redis, heartbeat_interval: int, group_name, consumer_name):
        assert self.task_heartbeat is None
        task_heartbeat = self._task_send_heartbeat(client_redis, heartbeat_interval, group_name, consumer_name)
        self.task_heartbeat = asyncio.create_task(task_heartbeat)

    async def remove_message_from_pending(self, client_redis, group_name):
        await client_redis.xack(self.stream, group_name, self.message_id)
        self.task_heartbeat.cancel()
        self.task_heartbeat = None


