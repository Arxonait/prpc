import pytest
import pytest_asyncio
import redis

from main.brokers.redis import RedisClientBroker, RedisAdminBroker, RedisServerBroker
from main.prpcmessage import PRPCMessage
from tests.data_for_tests import CONFIG_BROKER_REDIS, TEST_NAME_QUEUE
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


@pytest.mark.asyncio(loop_scope="class")
class TestRedisStream:
    def test_client_add_task_in_stream(self, client_redis, clear_redis):
        task = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        client = RedisClientBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        client.add_message_in_queue(task)

        stream_data = client_redis.xread({client.get_queue_name(): 0}, count=1)
        stream_data = parse_stream_data(stream_data)

        assert len(stream_data) == 1
        task_from_stream = PRPCMessage.deserialize(stream_data[0]["data"][b"serialized_data"])
        assert task.message_id == task_from_stream.message_id

    async def test_multiply_create_groups(self, client_redis, clear_redis):
        admin_broker = RedisAdminBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await admin_broker.init()
        await admin_broker.init()

    async def test_server_get_task_from_stream(self, client_redis, clear_redis):
        task = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        client_redis.xadd(redis_server_broker.get_queue_name(), {"serialized_data": task.serialize()})

        task_from_stream = await redis_server_broker.get_next_message_from_queue()
        assert task.message_id == task_from_stream.message_id

    async def test_server_client_send_feedback_and_get_from_feedback_message(self, client_redis, clear_redis, redis_server_broker, client_redis_broker):
        task = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        client_redis_broker.add_message_in_queue(task)

        task_from_stream = await redis_server_broker.get_next_message_from_queue()
        await redis_server_broker.add_message_in_feedback_queue(task_from_stream)

        task_from_feedback = client_redis.get(redis_server_broker.get_queue_feedback_name_message_id(task.message_id))
        task_from_feedback = PRPCMessage.deserialize(task_from_feedback)
        assert task.message_id == task_from_feedback.message_id

        task_from_feedback = client_redis_broker.search_message_in_feedback(task)
        assert task.message_id == task_from_feedback.message_id


