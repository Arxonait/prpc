import json

import pytest
import redis

from pytest import mark

from main.broker_manager import QueueWithFeedbackRedis
from main.task import Task


@pytest.fixture(scope="class")
def config_broker_redis():
    return {
        "host": "localhost",
        "port": 6379,
        "db": 0
    }


@pytest.fixture(scope="class")
def name_queue():
    return "test_queue"


@pytest.fixture
def create_task_in_queue_redis(queue_redis):
    task = Task(name_func="test_func", func_args=[], func_kwargs=[])

    queue = queue_redis
    queue.add_task_in_queue(task)
    return task


@pytest.fixture(scope="class")
def queue_redis(config_broker_redis, name_queue):
    queue = QueueWithFeedbackRedis(config_broker_redis, name_queue)
    return queue


@pytest.fixture(scope="class")
def client_redis(config_broker_redis):
    client = redis.Redis(**config_broker_redis)
    return client


class TestRedisBroker:
    def test_add_task_in_queue(self, queue_redis, client_redis, name_queue):
        task = Task(name_func="test_func", func_args=[], func_kwargs=[])

        queue_redis.add_task_in_queue(task)

        task_data = json.loads(client_redis.lpop(name_queue))
        saved_task = Task(**task_data)

        assert task.task_id == saved_task.task_id

    def test_get_task_from_queue(self, queue_redis, client_redis, name_queue, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()

        assert isinstance(task, Task)
        assert create_task_in_queue_redis.task_id == task.task_id

        task_data = json.loads(client_redis.get(f"{name_queue}:in_process:{task.task_id}"))
        client_redis.delete(f"{name_queue}:in_process:{task.task_id}")
        task_processing = Task(**task_data)

        assert task_processing.task_id == create_task_in_queue_redis.task_id

    def test_save_task_in_feedback(self, queue_redis, client_redis, name_queue, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()

        queue_redis.add_task_in_feedback(task)

        task_data = client_redis.get(f"{name_queue}:in_process:{task.task_id}")
        assert task_data is None

        task_data = json.loads(client_redis.get(f"{name_queue}:feedback:{task.task_id}"))
        client_redis.delete(f"{name_queue}:feedback:{task.task_id}")
        task_processing = Task(**task_data)
        assert task_processing.task_id == create_task_in_queue_redis.task_id

    def test_search_task_in_feedback(self, queue_redis, client_redis, name_queue, create_task_in_queue_redis):
        task = queue_redis.get_next_task_in_queue()
        queue_redis.add_task_in_feedback(task)

        task = queue_redis.search_task_in_feedback(task.task_id)

        task_data = client_redis.get(f"{name_queue}:feedback:{task.task_id}")
        assert task_data is None

        assert task.task_id == create_task_in_queue_redis.task_id



