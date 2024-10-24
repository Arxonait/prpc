import asyncio
import datetime
import multiprocessing
import os
import time
import uuid
from typing import Callable, Literal, Union, Optional
from unittest.mock import patch

import pytest
import pytest_asyncio
import redis

from main.app_client import ping
from main.app_server import AppServer
from main.brokers.redis import RedisClientBroker, RedisServerBroker
from main.func_module import FuncDataServer
from main.task import Task
from main.type_module import CheckerValueSerialize, HandlerAnnotation

TEST_NAME_QUEUE = "test_queue"
CONFIG_BROKER_REDIS = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}


@pytest.fixture()
def set_env_client_data():
    os.environ["PRPC_TYPE_BROKER"] = "redis"
    os.environ["PRPC_URL_BROKER"] = "redis://localhost:6379/0"
    os.environ["PRPC_QUEUE_NAME"] = TEST_NAME_QUEUE


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
    queue = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
    await queue.init()
    return queue


@pytest.fixture(scope="class")
def client_queue_redis(client_redis):
    queue = RedisClientBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
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
    app = AppServer(type_broker="redis", broker_url={
        "host": "localhost",
        "port": 6379,
        "db": 0
    }, default_type_worker="thread", max_number_worker=max_number_worker, timeout_worker=timeout_worker,
                    name_queue=TEST_NAME_QUEUE)
    app.register_funcs(func_for_test_hello_world, func_for_test_sum, func_for_test_greeting,
                       func_for_test_long_task, func_for_test_task_5s, func_for_test_task_custom_sec,
                       worker_type="thread")
    app.start()


@pytest.fixture(scope="function")
def server_redis_thread_4w_5s():
    process = multiprocessing.Process(target=__process_server_redis_thread, args=(datetime.timedelta(seconds=5), 4))
    process.start()
    yield process
    process.terminate()


@pytest.fixture(scope="function")
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

        task_data = client_redis.lpop(client_queue_redis.get_queue_name())
        saved_task = Task.deserialize(task_data)

        assert task.task_id == saved_task.task_id

    async def test_get_task_from_queue(self, server_queue_redis, client_redis, create_task_in_queue_redis, clear_redis):
        created_task: Task = create_task_in_queue_redis
        server_queue_redis: RedisServerBroker = server_queue_redis
        task: Task = await server_queue_redis.get_next_task_from_queue()

        assert client_redis.llen(server_queue_redis.get_queue_name()) == 0

        assert isinstance(task, Task)
        assert created_task.task_id == task.task_id

        task_data = client_redis.get(server_queue_redis.get_queue_process_name_task_id(task.task_id))
        task_processing = Task.deserialize(task_data)

        assert task_processing.task_id == created_task.task_id

    async def test_save_task_in_feedback(self, server_queue_redis, client_redis, create_task_in_queue_redis, clear_redis):
        created_task: Task = create_task_in_queue_redis
        server_queue_redis: RedisServerBroker = server_queue_redis
        task = await server_queue_redis.get_next_task_from_queue()

        await server_queue_redis.add_task_in_feedback_queue(task)

        task_data = client_redis.get(server_queue_redis.get_queue_process_name_task_id(task.task_id))
        assert task_data is None

        task_data = client_redis.get(server_queue_redis.get_queue_feedback_name_task_id(task.task_id))
        task_feedback = Task.deserialize(task_data)
        assert task_feedback.task_id == created_task.task_id

    async def test_search_task_in_feedback(self, server_queue_redis, client_queue_redis, client_redis,
                                           create_task_in_queue_redis):
        created_task: Task = create_task_in_queue_redis
        server_queue_redis: RedisServerBroker = server_queue_redis
        task = await server_queue_redis.get_next_task_from_queue()

        await server_queue_redis.add_task_in_feedback_queue(task)

        task = client_queue_redis.search_task_in_feedback(task)

        task_data = client_redis.get(server_queue_redis.get_queue_feedback_name_task_id(task.task_id))
        assert task_data is None

        assert task.task_id == created_task.task_id

    async def test_restore_tasks(self, create_task_in_queue_redis, clear_redis, client_redis):
        queue = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await queue.init()
        await queue.get_next_task_from_queue()
        del queue
        queue = RedisServerBroker(CONFIG_BROKER_REDIS, TEST_NAME_QUEUE)
        await queue.init()

        assert client_redis.llen(queue.get_queue_name()) == 1


class TestAppServerRedisThread:
    @pytest.mark.parametrize(
        "func_name, args, kwargs, expected",
        [
            ("func_for_test_hello_world", (), {}, "hello world"),
            ("func_for_test_sum", (5, 6), {}, 11),
            ("func_for_test_greeting", (), {"name": "dasha", "part_of_day": "day"}, "Hi dasha, have a nice day")
        ]
    )
    def test_run_funcs(self, func_name, args, kwargs, expected, server_redis_thread_4w_5s, client_queue_redis,
                       clear_redis):
        task = Task(func_name=func_name, func_args=args, func_kwargs=kwargs)

        client_queue_redis.add_task_in_queue(task)
        while True:
            task_result = client_queue_redis.search_task_in_feedback(task)
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
            task_result = client_queue_redis.search_task_in_feedback(task)
            if task_result:
                assert task_result.result is None
                assert task_result.exception_info is not None
                break
            time.sleep(0.5)

    def test_max_number_worker(self, server_redis_thread_2w_20s, client_queue_redis, client_redis, clear_redis,
                               set_env_client_data):
        task_ping = ping()
        task_ping.sync_wait_result_task(datetime.timedelta(seconds=10))

        tasks_id = []
        for _ in range(6):
            task = Task(func_name="func_for_test_task_custom_sec", func_args=(15,), func_kwargs={})
            client_queue_redis.add_task_in_queue(task)
            tasks_id.append(task.task_id)

        assert 6 - 2 == client_redis.llen("prpc_" + TEST_NAME_QUEUE)
        assert client_redis.get(client_queue_redis.get_queue_process_name_task_id(tasks_id[1]))
        assert client_redis.get(client_queue_redis.get_queue_process_name_task_id(tasks_id[1]))
        assert client_redis.get(client_queue_redis.get_queue_process_name_task_id(tasks_id[1])) is None

    def test_done_all_task(self, server_redis_thread_2w_20s, client_queue_redis, client_redis, clear_redis):
        tasks_id = []
        seconds = [5, 1, 1, 2, 3, 1]
        for secs in seconds:
            task = Task(func_name="func_for_test_task_custom_sec", func_args=(secs,), func_kwargs={})
            client_queue_redis.add_task_in_queue(task)
            tasks_id.append(task.task_id)

        start_wait = datetime.datetime.now()
        while True:
            keys = client_redis.keys(f"prpc_feedback_{TEST_NAME_QUEUE}*")
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
            FuncDataServer(func, worker_type)
        except Exception as e:
            assert is_raise_exception
        else:
            assert not is_raise_exception


class TestCheckerValueSerialize:
    @pytest.mark.parametrize(
        "value, is_good_value",
        [
            (23, True),
            ("str", True),
            (False, True),
            (None, True),
            (23.5, True),
            ([23, False], True),
            (datetime.timedelta(23), True),
            (datetime.datetime.now(), True),
            (uuid.uuid4(), True),
            (CheckerValueSerialize(), False),
            (dict, True)
        ]
    )
    def test_checker_value_serialize(self, value, is_good_value):
        assert CheckerValueSerialize.is_value_good_for_serialize(value)[0] == is_good_value


class TestHandlerAnnotation:
    @pytest.mark.parametrize(
        "annotation, is_valid_annotation",
        [
            (int, True),
            (str, True),
            (bool, True),
            (type(None), True),
            (None, True),
            (float, True),
            (list, True),
            (list[int], True),
            (list[Callable], True),
            (list[(...)], True),
            (Literal["value1", "value2"], True),
            (int | float | None, True),
            (Union[int, float], True),
            (Optional[int], True),
            (datetime.datetime, True),
            (datetime.timedelta, True),
            (uuid.UUID, True),
            (list[int, HandlerAnnotation], False)
        ]
    )
    def test_is_valid_annotation(self, annotation, is_valid_annotation):
        assert (len(HandlerAnnotation.is_valid_annotation(annotation)) == 0) == is_valid_annotation

    @pytest.mark.parametrize(
        "annotation, annotation_serialized",
        [
            (int, "int"),
            (str, "str"),
            (bool, "bool"),
            (None, "None"),
            (float, "float"),
            (list, "list"),
            (list[int], "list[int]"),
            (list[Callable], "list[Callable]"),
            (list[()], "list[()]"),
            ((), "()"),
            (Literal["value1", "value2"], "Literal['value1', 'value2']"),
            (int | float | None, "int | float | None"),
            (Union[int, float], "Union[int, float]"),
            (Optional[int], "Optional[int]"),
            (datetime.datetime, "datetime"),
            (datetime.timedelta, "timedelta"),
            (uuid.UUID, "UUID"),
        ]
    )
    def test_serialize(self, annotation, annotation_serialized):
        assert HandlerAnnotation.serialize_annotation(annotation) == annotation_serialized