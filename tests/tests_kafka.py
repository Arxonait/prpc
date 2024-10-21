import kafka
import pytest
import pytest_asyncio
from kafka import KafkaAdminClient

from main.brokers_module import ServerQueueKafka
from main.task import Task

broker_config = "localhost:9092"
queue_name = "test_prpc_0"


@pytest_asyncio.fixture(loop_scope="class")
async def server_queue_kafka():
    queue = ServerQueueKafka(broker_config, queue_name)
    await queue.init()
    return queue


@pytest.fixture(scope="class")
def consumer_feedback_kafka():
    return kafka.KafkaConsumer(
        f"prpc_feedback_{queue_name}",
        bootstrap_servers=broker_config,
        group_id="prpc_test_group",
        auto_offset_reset='earliest'
    )


@pytest.fixture(scope="class")
def producer_kafka():
    return kafka.KafkaProducer(bootstrap_servers=broker_config)


@pytest.fixture
def clear_kafka():
    admin_client = KafkaAdminClient(bootstrap_servers=broker_config)
    yield
    try:
        admin_client.delete_topics([f"prpc_{queue_name}", f"prpc_feedback_{queue_name}"])
    except Exception as e:
        print(e)
    admin_client.close()


@pytest.mark.asyncio(loop_scope="class")
class TestKafkaBroker:

    async def test_get_task_from_queue(self, producer_kafka, clear_kafka, server_queue_kafka):
        task = Task(func_name="test_func", func_args=[], func_kwargs={})
        producer_kafka.send(f"prpc_{queue_name}", task.serialize().encode())

        task_new = await server_queue_kafka.get_next_task_from_queue()
        assert task.task_id == task_new.task_id

    async def test_add_task_in_feedback_queue(self, consumer_feedback_kafka, clear_kafka, server_queue_kafka):
        task = Task(func_name="test_func", func_args=[], func_kwargs={})
        await server_queue_kafka.add_task_in_feedback_queue(task)

        for message in consumer_feedback_kafka:
            task_new = Task.deserialize(message.value)
            if task_new.task_id == task.task_id:
                assert True
                break
            else:
                assert False


