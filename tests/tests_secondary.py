import asyncio
import datetime
import time
import uuid
from typing import Callable, Literal, Union, Optional

import pytest

from main.func_module import FuncDataServer
from main.type_module import CheckerValueSerialize, HandlerAnnotation


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