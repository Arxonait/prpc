import pytest
import redis

from tests.data_for_tests import CONFIG_BROKER_REDIS, TEST_NAME_QUEUE


def _delete_keys_by_pattern(redis_client, pattern):
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
        if keys:
            redis_client.delete(*keys)
        if cursor == 0:
            break


@pytest.fixture(scope="class")
def client_redis():
    client = redis.from_url(CONFIG_BROKER_REDIS)
    return client


@pytest.fixture()
def clear_redis(client_redis):
    yield
    _delete_keys_by_pattern(redis_client=client_redis, pattern=f"*{TEST_NAME_QUEUE}*")
    print("Произошла очистка редиса")


@pytest.fixture(scope="class")
def clear_redis_class(client_redis):
    yield
    _delete_keys_by_pattern(redis_client=client_redis, pattern=f"*{TEST_NAME_QUEUE}*")
    print("Произошла очистка редиса - class")
