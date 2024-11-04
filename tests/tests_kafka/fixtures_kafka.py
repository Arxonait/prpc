import kafka
import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from tests.data_for_tests import FRAMEWORK_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_FEEDBACK

BROKER_URL = "localhost:9092"


@pytest.fixture
def clear_kafka():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER_URL)

    yield

    try:
        admin_client.delete_topics([FRAMEWORK_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_FEEDBACK])
    except Exception as e:
        print(e)
    admin_client.close()


@pytest.fixture
def create_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER_URL)

    topic_list = [
        NewTopic(name=FRAMEWORK_NAME_QUEUE, num_partitions=8, replication_factor=1),
        NewTopic(name=FRAMEWORK_NAME_QUEUE_FEEDBACK, num_partitions=2, replication_factor=1),
    ]

    admin_client.create_topics(new_topics=topic_list, validate_only=False)


def _consumer_kafka(topics):
    return kafka.KafkaConsumer(
        *topics,
        bootstrap_servers=BROKER_URL,
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000
    )


@pytest.fixture(scope="class")
def consumer_kafka_feedback():
    return _consumer_kafka(FRAMEWORK_NAME_QUEUE_FEEDBACK)


@pytest.fixture(scope="class")
def consumer_kafka_main_queue():
    return _consumer_kafka(FRAMEWORK_NAME_QUEUE)


@pytest.fixture(scope="class")
def producer_kafka():
    return kafka.KafkaProducer(bootstrap_servers=BROKER_URL)


