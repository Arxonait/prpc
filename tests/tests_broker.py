from prpc.brokers.redis import RedisServerBroker
from tests.data_for_tests import FRAMEWORK_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_FEEDBACK, CONFIG_BROKER_REDIS, \
    TEST_NAME_QUEUE, GROUP_NAME
from tests.tests_redis.fixtures_redis import clear_redis, client_redis
from tests.tests_redis.tests_redis import client_redis_broker, redis_server_broker, init_admin_broker


class TestAbstractBroker:
    def test_framework_name_stream(self, client_redis_broker, clear_redis):
        assert FRAMEWORK_NAME_QUEUE == client_redis_broker.queue.queue
        assert FRAMEWORK_NAME_QUEUE_FEEDBACK == client_redis_broker.queue.queue_feedback

    def test_server_broker_unic_queue_number(self, clear_redis):

        redis_server_broker0 = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, GROUP_NAME, None)
        redis_server_broker1 = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, GROUP_NAME, None)
        redis_server_broker2 = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, GROUP_NAME, None)

        assert redis_server_broker0._consumer_number == 0
        assert redis_server_broker1._consumer_number == 1
        assert redis_server_broker2._consumer_number == 2
        assert RedisServerBroker._count_instance == 3
