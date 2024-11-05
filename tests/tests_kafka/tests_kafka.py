import json
import uuid

import pytest
import pytest_asyncio

from main.brokers.kafka import KafkaServerBroker, KafkaClientBroker
from main.prpcmessage import PRPCMessage
from tests.data_for_tests import TEST_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_FEEDBACK, FRAMEWORK_NAME_QUEUE, GROUP_NAME, \
    FRAMEWORK_NAME_QUEUE_RAW
from tests.tests_kafka.fixtures_kafka import BROKER_URL, clear_kafka, consumer_kafka_main_queue, producer_kafka, consumer_kafka_feedback


@pytest_asyncio.fixture(loop_scope="class")
async def server_broker_kafka():
    queue = KafkaServerBroker(BROKER_URL, TEST_NAME_QUEUE, GROUP_NAME)
    await queue.init()
    return queue


@pytest.fixture(scope="class")
def client_broker_kafka():
    queue = KafkaClientBroker(BROKER_URL, TEST_NAME_QUEUE)
    return queue


@pytest.fixture()
def add_message_in_topic(producer_kafka):
    message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
    producer_kafka.send(FRAMEWORK_NAME_QUEUE, message.serialize().encode())
    return message


@pytest.mark.asyncio(loop_scope="class")
class TestKafkaBroker:

    def test_add_task_in_queue(self, clear_kafka, client_broker_kafka, consumer_kafka_main_queue):
        message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        client_broker_kafka.add_message_in_queue(message)

        for message in consumer_kafka_main_queue:
            message_from_kafka = PRPCMessage.deserialize(message.value)
            if message_from_kafka.message_id == message.message_id:
                assert True
                break
            else:
                assert False

    async def test_get_task_from_queue(self, server_broker_kafka, add_message_in_topic, clear_kafka):
        message_next = await server_broker_kafka.get_next_message_from_queue()
        assert add_message_in_topic.message_id == message_next.message_id

    async def test_add_task_in_feedback_queue(self, server_broker_kafka, add_message_in_topic, consumer_kafka_feedback, clear_kafka):
        message_next = await server_broker_kafka.get_next_message_from_queue()
        await server_broker_kafka.add_message_in_feedback_queue(message_next)

        for message in consumer_kafka_feedback:
            message_from_kafka = PRPCMessage.deserialize(message.value)
            if message_from_kafka.message_id == message_next.message_id:
                assert True
                break
            else:
                assert False

    def test_get_task_from_feedback_queue(self, clear_kafka, client_broker_kafka, producer_kafka):
        message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        producer_kafka.send(FRAMEWORK_NAME_QUEUE_FEEDBACK, message.serialize().encode())

        message_done = client_broker_kafka.search_message_in_feedback(message)

        assert isinstance(message_done, PRPCMessage)
        assert message.message_id == message_done.message_id

    def test_get_several_tasks_from_feedback_queue_one_consumer(self, clear_kafka, producer_kafka):
        message1 = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        message2 = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})

        producer_kafka.send(FRAMEWORK_NAME_QUEUE_FEEDBACK, message1.serialize().encode())
        producer_kafka.send(FRAMEWORK_NAME_QUEUE_FEEDBACK, message2.serialize().encode())
        producer_kafka.flush()
        client = KafkaClientBroker(BROKER_URL, TEST_NAME_QUEUE)

        message_new2 = client.search_message_in_feedback(message2)
        assert message2.message_id == message_new2.message_id

        message_new1 = client.search_message_in_feedback(message1)
        assert message_new1 is None

    async def test_server_get_prpc_message_from_topic_input_data_python(self, producer_kafka, clear_kafka, server_broker_kafka):
        message = PRPCMessage(func_name="test_func", func_args=[], func_kwargs={})
        producer_kafka.send(FRAMEWORK_NAME_QUEUE, message.serialize().encode())

        message_from_stream = await server_broker_kafka.get_next_message_from_queue()
        assert message.message_id == message_from_stream.message_id
        assert message.__dict__ == message_from_stream.__dict__

    async def test_server_get_prpc_message_from_topic_input_data_raw(self, producer_kafka, clear_kafka, server_broker_kafka):
        message = {"func_name": "hello_world", "message_id": str(uuid.uuid4())}
        producer_kafka.send(FRAMEWORK_NAME_QUEUE_RAW, json.dumps(message).encode())

        message_from_stream = await server_broker_kafka.get_next_message_from_queue()
        assert message["message_id"] == str(message_from_stream.message_id)
        message.pop("message_id")
        for key in message:
            assert message[key] == getattr(message_from_stream, key)


