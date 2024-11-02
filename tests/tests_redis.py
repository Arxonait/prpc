import time

import pytest
import pytest_asyncio

from main.brokers.redis import RedisClientBroker, RedisAdminBroker, RedisServerBroker
from main.prpcmessage import PRPCMessage
from main.settings_server import Settings
from tests.data_for_tests import CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, FRAMEWORK_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_FEEDBACK
from tests.fixtures_redis import client_redis, clear_redis


def parse_stream_data(stream_data):
    parsed_data = []
    for stream_name, messages in stream_data:
        for message_id, message in messages:
            parsed_data.append({
                "stream": stream_name,
                "id": message_id,
                "data": message
            })
    return parsed_data


@pytest_asyncio.fixture(loop_scope="class")
async def init_admin_broker():
    admin_broker = RedisAdminBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    await admin_broker.init()
    return admin_broker


@pytest_asyncio.fixture(loop_scope="class")
async def redis_server_broker(init_admin_broker):
    redis_server_broker = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, context={"queue_number": 0})
    await redis_server_broker.init()
    return redis_server_broker


@pytest.fixture(scope="class")
def client_redis_broker():
    client = RedisClientBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    return client


@pytest.fixture()
def add_message_in_stream(client_redis):
    message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
    client_redis.xadd(FRAMEWORK_NAME_QUEUE, {"message": message.serialize()})
    return message


@pytest.mark.asyncio(loop_scope="class")
class TestRedisStream:
    def test_client_add_message_in_stream(self, client_redis, clear_redis):
        message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        client = RedisClientBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        client.add_message_in_queue(message)

        stream_data = client_redis.xread({FRAMEWORK_NAME_QUEUE: 0}, count=1)
        stream_data = parse_stream_data(stream_data)

        assert len(stream_data) == 1
        message_from_stream = PRPCMessage.deserialize(stream_data[0]["data"][b"message"])
        assert message.message_id == message_from_stream.message_id

    async def test_multiply_create_groups(self, client_redis, clear_redis):
        admin_broker = RedisAdminBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await admin_broker.init()
        await admin_broker.init()

    async def test_server_get_task_from_stream(self, clear_redis, redis_server_broker, add_message_in_stream):
        message = add_message_in_stream
        message_from_stream = await redis_server_broker.get_next_message_from_queue()

        assert message.message_id == message_from_stream.message_id

    async def test_server_client_send_feedback_and_get_from_feedback_message(self, client_redis, clear_redis,
                                                                             redis_server_broker, client_redis_broker,
                                                                             add_message_in_stream):
        message = add_message_in_stream
        message_from_stream = await redis_server_broker.get_next_message_from_queue()
        await redis_server_broker.add_message_in_feedback_queue(message_from_stream)

        message_from_feedback = client_redis.get(f"{FRAMEWORK_NAME_QUEUE_FEEDBACK}_{message.message_id}")
        message_from_feedback = PRPCMessage.deserialize(message_from_feedback)
        assert message.message_id == message_from_feedback.message_id

        message_from_feedback = client_redis_broker.search_message_in_feedback(message)
        assert message.message_id == message_from_feedback.message_id

    async def test_server_restore_message(self, init_admin_broker, add_message_in_stream, clear_redis):
        Settings._redis_recover_interval = 1
        Settings._redis_heartbeat_interval = 100

        redis_server_broker0 = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, context={"queue_number": 0})
        await redis_server_broker0.init()
        message0 = await redis_server_broker0.get_next_message_from_queue()
        del redis_server_broker0

        time.sleep(3)

        redis_server_broker1 = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, context={"queue_number": 1})
        await redis_server_broker1.init()
        message1 = await redis_server_broker1.get_next_message_from_queue()

        assert message1.message_id == message0.message_id


def test_framework_name_stream(client_redis_broker, clear_redis):
    assert FRAMEWORK_NAME_QUEUE == client_redis_broker.queue.queue
    assert FRAMEWORK_NAME_QUEUE_FEEDBACK == client_redis_broker.queue.queue_feedback



