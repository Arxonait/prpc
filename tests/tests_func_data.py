import asyncio
import time

import pytest

from prpc.func_data import FuncDataServer


def func_for_test_hello_world():
    time.sleep(1)
    return "hello world"


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
