import asyncio
import datetime
import json
import logging
import multiprocessing
import threading
import time

import pytest
import pytest_asyncio
import redis

from pytest import mark

from main.app_server import AppServer
from main.brokers_module import ServerQueueRedis, ClientQueueRedisSync
from main.func_module import FuncData
from main.task import Task

TEST_NAME_QUEUE = "test_queue"
CONFIG_BROKER_REDIS = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}


def delete_keys_by_pattern(redis_client, pattern):
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
        if keys:
            redis_client.delete(*keys)
        if cursor == 0:
            break


@pytest_asyncio.fixture(loop_scope="class")
async def create_task_in_queue_redis(client_queue_redis):
    task = Task(func_name="test_func", func_args=[], func_kwargs={})
    client_queue_redis.add_task_in_queue(task)
    return task


@pytest_asyncio.fixture(loop_scope="class")
async def server_queue_redis(client_redis):
    client_redis.delete("prpc:" + TEST_NAME_QUEUE)
    queue = ServerQueueRedis(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    await queue.init()
    return queue


@pytest.fixture(scope="class")
def client_queue_redis(client_redis):
    client_redis.delete("prpc:" + TEST_NAME_QUEUE)
    queue = ClientQueueRedisSync(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    queue.init()
    return queue


@pytest.fixture(scope="class")
def client_redis():
    client = redis.Redis(**CONFIG_BROKER_REDIS)
    return client


@pytest.fixture
def clear_redis(client_redis):
    yield
    delete_keys_by_pattern(redis_client=client_redis, pattern=f"*{TEST_NAME_QUEUE}*")


def func_for_test_hello_world():
    time.sleep(1)
    return "hello world"


def func_for_test_sum(a, b):
    time.sleep(1)
    return a + b


def func_for_test_greeting(name, part_of_day):
    time.sleep(1)
    return f"Hi {name}, have a nice {part_of_day}"


def func_for_test_long_task():
    time.sleep(100)
    return "done"


def func_for_test_task_5s():
    time.sleep(5)
    return "done"


def func_for_test_task_custom_sec(secs):
    time.sleep(secs)
    return "done"


def __process_server_redis_thread(timeout_worker, max_number_worker):
    app = AppServer(type_broker="redis", config_broker={
        "host": "localhost",
        "port": 6379,
        "db": 0
    }, default_type_worker="thread",
                    max_number_worker=max_number_worker, name_queue=TEST_NAME_QUEUE, timeout_worker=timeout_worker)
    app.register_funcs(func_for_test_hello_world, func_for_test_sum, func_for_test_greeting,
                       func_for_test_long_task, func_for_test_task_5s, func_for_test_task_custom_sec)
    app.start()


@pytest.fixture(scope="session")
def server_redis_thread_4w_5s():
    process = multiprocessing.Process(target=__process_server_redis_thread, args=(datetime.timedelta(seconds=5), 4))
    process.start()
    yield process
    process.terminate()


@pytest.fixture(scope="session")
def server_redis_thread_2w_20s():
    process = multiprocessing.Process(target=__process_server_redis_thread, args=(datetime.timedelta(seconds=20), 2))
    process.start()
    yield process
    process.terminate()


@pytest.mark.asyncio(loop_scope="class")
class TestRedisBroker:
    def test_add_task_in_queue(self, client_queue_redis, client_redis, clear_redis):
        task = Task(func_name="test_func", func_args=[], func_kwargs={})

        client_queue_redis.add_task_in_queue(task)

        task_data = json.loads(client_redis.lpop("prpc:" + TEST_NAME_QUEUE))
        saved_task = Task(**task_data)

        assert task.task_id == saved_task.task_id

    async def test_get_task_from_queue(self, server_queue_redis, client_redis, create_task_in_queue_redis, clear_redis):
        created_task = create_task_in_queue_redis
        task = await server_queue_redis.get_next_task_from_queue()

        assert client_redis.llen("prpc:" + TEST_NAME_QUEUE) == 0

        assert isinstance(task, Task)
        assert created_task.task_id == task.task_id

        task_data = json.loads(client_redis.get(f"prpc:in_process:{TEST_NAME_QUEUE}:{task.task_id}"))
        task_processing = Task(**task_data)

        assert task_processing.task_id == created_task.task_id

    async def test_save_task_in_feedback(self, server_queue_redis, client_redis, create_task_in_queue_redis):
        created_task = create_task_in_queue_redis
        task = await server_queue_redis.get_next_task_from_queue()

        await server_queue_redis.add_task_in_feedback_queue(task)

        task_data = client_redis.get(f"prpc:in_process:{TEST_NAME_QUEUE}:{task.task_id}")
        assert task_data is None

        task_data = json.loads(client_redis.get(f"prpc:feedback:{TEST_NAME_QUEUE}:{task.task_id}"))
        client_redis.delete(f"prpc:feedback:{TEST_NAME_QUEUE}:{task.task_id}")
        task_processing = Task(**task_data)
        assert task_processing.task_id == created_task.task_id

    async def test_search_task_in_feedback(self, server_queue_redis, client_queue_redis, client_redis,
                                           create_task_in_queue_redis):
        created_task = create_task_in_queue_redis
        task = await server_queue_redis.get_next_task_from_queue()

        await server_queue_redis.add_task_in_feedback_queue(task)

        task = client_queue_redis.search_task_in_feedback(task.task_id)

        task_data = client_redis.get(f"prpc:feedback:{TEST_NAME_QUEUE}:{task.task_id}")
        assert task_data is None

        assert task.task_id == created_task.task_id

    async def test_restore_tasks(self, client_queue_redis, clear_redis, client_redis):
        task = Task(func_name="test_func", func_args=[], func_kwargs={})
        client_queue_redis.add_task_in_queue(task)

        queue = ServerQueueRedis(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await queue.init()
        await queue.get_next_task_from_queue()
        del queue
        queue = ServerQueueRedis(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await queue.init()

        assert client_redis.llen("prpc:" + TEST_NAME_QUEUE) == 1


class TestAppServerRedisThread:
    @pytest.mark.parametrize(
        "func_name, args, kwargs, expected",
        [
            ("func_for_test_hello_world", (), {}, "hello world"),
            ("func_for_test_sum", (5, 6), {}, 11),
            ("func_for_test_greeting", (), {"name": "dasha", "part_of_day": "day"}, "Hi dasha, have a nice day")
        ]
    )
    def test_run_funcs(self, func_name, args, kwargs, expected, server_redis_thread_4w_5s, client_queue_redis):
        task = Task(func_name=func_name, func_args=args, func_kwargs=kwargs)

        client_queue_redis.add_task_in_queue(task)
        while True:
            task_result = client_queue_redis.search_task_in_feedback(task.task_id)
            if task_result:
                assert task_result.result == expected
                break
            time.sleep(0.5)

    @pytest.mark.parametrize(
        "func_name, args, kwargs",
        [
            ("func_for_test_hello", (), {}),
            ("func_for_test_sum", (5, None), {}),
            ("func_for_test_long_task", (), {}),
            ("func_for_test_greeting", ("Dasha",), {})
        ]
    )
    def test_run_wrong_funcs(self, func_name, args, kwargs, server_redis_thread_4w_5s, client_queue_redis, clear_redis):
        task = Task(func_name=func_name, func_args=args, func_kwargs=kwargs)

        client_queue_redis.add_task_in_queue(task)
        while True:
            task_result = client_queue_redis.search_task_in_feedback(task.task_id)
            if task_result:
                assert task_result.result is None
                assert task_result.exception_info is not None
                break
            time.sleep(0.5)

    def test_max_number_worker(self, server_redis_thread_2w_20s, client_queue_redis, client_redis, clear_redis):
        tasks_id = []
        for _ in range(6):
            task = Task(func_name="func_for_test_task_5s", func_args=(), func_kwargs={})
            client_queue_redis.add_task_in_queue(task)
            tasks_id.append(task.task_id)

        time.sleep(1)
        assert 6 - 2 == client_redis.llen("prpc:" + TEST_NAME_QUEUE)
        assert client_redis.get(f"prpc:in_process:{TEST_NAME_QUEUE}:{tasks_id[0]}")
        assert client_redis.get(f"prpc:in_process:{TEST_NAME_QUEUE}:{tasks_id[1]}")
        assert client_redis.get(f"prpc:in_process:{TEST_NAME_QUEUE}:{tasks_id[2]}") is None

    def test_done_all_task(self, server_redis_thread_2w_20s, client_queue_redis, client_redis, clear_redis):
        tasks_id = []
        seconds = [5, 1, 1, 2, 3, 1]
        for secs in seconds:
            task = Task(func_name="func_for_test_task_custom_sec", func_args=(secs,), func_kwargs={})
            client_queue_redis.add_task_in_queue(task)
            tasks_id.append(task.task_id)

        start_wait = datetime.datetime.now()
        while True:
            keys = client_redis.keys(f"prpc:feedback:{TEST_NAME_QUEUE}:*")
            if len(keys) == len(seconds):
                break
            if datetime.datetime.now() - start_wait > datetime.timedelta(seconds=sum(seconds) + len(seconds) * 2):
                assert False


async def func_for_test_func_data_wait_hello():
    await asyncio.sleep(2)
    return "hello"


class TestFuncData:
    @pytest.mark.parametrize(
        "func, worker_type, is_raise_exception",
        [
            (func_for_test_hello_world, "thread", False),
            (func_for_test_hello_world, "process", False),
            (func_for_test_hello_world, "async", True),
            (func_for_test_func_data_wait_hello, "thread", True),
            (func_for_test_func_data_wait_hello, "process", True),
            (func_for_test_func_data_wait_hello, "async", False),
        ]
    )
    def test_check_ability_to_work_with_function(self, func, worker_type, is_raise_exception):
        try:
            FuncData(func, worker_type)
        except Exception as e:
            assert is_raise_exception
        else:
            assert not is_raise_exception
