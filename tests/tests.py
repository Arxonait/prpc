import json
import logging
import multiprocessing
import time

import pytest
import redis

from pytest import mark

from main.app import AppServer
from main.brokers_module import QueueWithFeedbackRedis
from main.task import Task

TEST_NAME_QUEUE = "test_queue"


@pytest.fixture(scope="class")
def config_broker_redis():
    return {
        "host": "localhost",
        "port": 6379,
        "db": 0
    }


@pytest.fixture(scope="class")
def name_queue():
    return TEST_NAME_QUEUE


@pytest.fixture
def create_task_in_queue_redis(queue_redis):
    task = Task(name_func="test_func", func_args=[], func_kwargs={})
    queue_redis.add_task_in_queue(task)
    return task


@pytest.fixture(scope="class")
def queue_redis(config_broker_redis, client_redis):
    client_redis.delete(TEST_NAME_QUEUE)
    queue = QueueWithFeedbackRedis(config_broker_redis, TEST_NAME_QUEUE, None, None)
    return queue


@pytest.fixture(scope="class")
def client_redis(config_broker_redis):
    client = redis.Redis(**config_broker_redis)
    return client


def func_for_test_hello_world():
    time.sleep(1)
    return "hello world"


def func_for_test_sum(a, b):
    time.sleep(1)
    return a + b


def func_for_test_greeting(name, part_of_day):
    time.sleep(1)
    return f"Hi {name}, have a nice {part_of_day}"


def __process_server_redis_thread():
    app = AppServer(type_broker="redis", config_broker={
        "host": "localhost",
        "port": 6379,
        "db": 0
    }, type_worker="thread", max_number_worker=4, name_queue=TEST_NAME_QUEUE)
    app.append_func(func_for_test_hello_world)
    app.append_func(func_for_test_sum)
    app.append_func(func_for_test_greeting)
    app.start()


@pytest.fixture(scope="session")
def server_redis_thread():
    process = multiprocessing.Process(target=__process_server_redis_thread)
    process.start()
    yield process
    process.terminate()


class TestRedisBroker:
    def test_add_task_in_queue(self, queue_redis, client_redis):
        task = Task(name_func="test_func", func_args=[], func_kwargs={})

        queue_redis.add_task_in_queue(task)

        task_data = json.loads(client_redis.lpop(TEST_NAME_QUEUE))
        saved_task = Task(**task_data)

        assert task.task_id == saved_task.task_id

    def test_get_task_from_queue(self, queue_redis, client_redis, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()

        assert isinstance(task, Task)
        assert create_task_in_queue_redis.task_id == task.task_id

        task_data = json.loads(client_redis.get(f"{TEST_NAME_QUEUE}:in_process:{task.task_id}"))
        client_redis.delete(f"{TEST_NAME_QUEUE}:in_process:{task.task_id}")
        task_processing = Task(**task_data)

        assert task_processing.task_id == create_task_in_queue_redis.task_id

    def test_save_task_in_feedback(self, queue_redis, client_redis, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()

        queue_redis.add_task_in_feedback(task)

        task_data = client_redis.get(f"{TEST_NAME_QUEUE}:in_process:{task.task_id}")
        assert task_data is None

        task_data = json.loads(client_redis.get(f"{TEST_NAME_QUEUE}:feedback:{task.task_id}"))
        client_redis.delete(f"{TEST_NAME_QUEUE}:feedback:{task.task_id}")
        task_processing = Task(**task_data)
        assert task_processing.task_id == create_task_in_queue_redis.task_id

    def test_search_task_in_feedback(self, queue_redis, client_redis, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()
        queue_redis.add_task_in_feedback(task)

        task = queue_redis.search_task_in_feedback(task.task_id)

        task_data = client_redis.get(f"{TEST_NAME_QUEUE}:feedback:{task.task_id}")
        assert task_data is None

        assert task.task_id == create_task_in_queue_redis.task_id


class TestAppServerRedisThread:
    @pytest.mark.parametrize(
        "func_name, args, kwargs, expected",
        [
            ("func_for_test_hello_world", (), {}, "hello world"),
            ("func_for_test_sum", (5, 6), {}, 11),
            ("func_for_test_greeting", (), {"name": "dasha", "part_of_day": "day"}, "Hi dasha, have a nice day")
        ]
    )
    def test_run_funcs(self, func_name, args, kwargs, expected, server_redis_thread, queue_redis):
        task = Task(name_func=func_name, func_args=args, func_kwargs=kwargs)

        queue_redis.add_task_in_queue(task)
        while True:
            task_result = queue_redis.search_task_in_feedback(task.task_id)
            if task_result:
                assert task_result.result == expected
                break
            time.sleep(0.5)


