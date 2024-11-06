import datetime
import json
import multiprocessing
import time
import uuid

import pytest

from prpc.app_server import AppServer
from prpc.brokers.redis import RedisClientBroker
from prpc.prpcmessage import PRPCMessage
from tests.data_for_tests import CONFIG_BROKER_REDIS, TEST_NAME_QUEUE, FRAMEWORK_NAME_QUEUE_RAW, \
    FRAMEWORK_NAME_QUEUE_FEEDBACK_RAW
from tests.tests_redis.fixtures_redis import client_redis, clear_redis_class


def run_app_server():
    app = AppServer(
        "redis",
        CONFIG_BROKER_REDIS,
        "thread",
        4,
        datetime.timedelta(seconds=4),
        "test_test"
    )

    @app.decorator_reg_func()
    def hello_world():
        time.sleep(0.5)
        return "hello_world"

    @app.decorator_reg_func()
    def func_time(time_sec):
        time.sleep(time_sec)
        return True

    app.start()


@pytest.fixture(scope="class")
def start_server():
    process = multiprocessing.Process(target=run_app_server)
    process.start()
    time.sleep(3)

    assert process.is_alive(), "Ошибка при запуске сервера."
    print("Сервер успешно запущен.")
    yield process

    process.terminate()
    process.join()
    print("Сервер успешно завершен.")


@pytest.fixture(scope="class")
def client_redis_broker():
    client = RedisClientBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    return client


class TestAppServerRedisThread:

    def test_run_func(self, start_server, client_redis_broker, clear_redis_class):
        task = PRPCMessage(func_name="hello_world", func_args=(), func_kwargs={})
        expected = "hello_world"

        client_redis_broker.add_message_in_queue(task)
        for _ in range(20):
            task_result = client_redis_broker.search_message_in_feedback(task)
            if task_result:
                assert task_result.result == expected
                break
            time.sleep(0.5)
        else:
            assert False, "вышло время ожидания"

    def test_done_all_task(self, start_server, client_redis_broker, client_redis, clear_redis_class):
        tasks_id = []
        seconds = [3, 3, 1, 2, 2, 3]
        for secs in seconds:
            task = PRPCMessage(func_name="func_time", func_args=(secs,), func_kwargs={})
            client_redis_broker.add_message_in_queue(task)
            tasks_id.append(task.message_id)

        for _ in range(30):
            keys = client_redis.keys(f"prpc_feedback_{TEST_NAME_QUEUE}*")
            if len(keys) == len(seconds):
                break
            time.sleep(0.5)
        else:
            assert False, "вышло время ожидания"