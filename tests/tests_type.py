import datetime
import uuid
import pytest
from typing import Literal, Optional, Union, Callable

from main.handlers_type import HandlerAnnotation, CheckerValueSerialize


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
            (datetime.timedelta(23), False),
            (datetime.datetime.now(), False),
            (uuid.uuid4(), False),
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
            (datetime.datetime, False),
            (datetime.timedelta, False),
            (uuid.UUID, False),
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
        ]
    )
    def test_serialize(self, annotation, annotation_serialized):
        assert HandlerAnnotation.serialize_annotation(annotation) == annotation_serialized