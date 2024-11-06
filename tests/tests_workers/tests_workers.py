import asyncio
from datetime import timedelta

import pytest

from prpc.prpcmessage import PRPCMessage
from prpc.workers.worker_future import ThreadWorker
from tests.data_for_tests import hello_world, summ, greeting, func_time


@pytest.mark.asyncio(loop_scope="class")
class TestWorker:
    @pytest.mark.parametrize(
        "func, message, timeout, result",
        [
            (hello_world, PRPCMessage(hello_world.__name__, (), {}), timedelta(seconds=3), "hello_world"),
            (summ, PRPCMessage(summ.__name__, (5, 6), {}), timedelta(seconds=3), 11),
            (greeting, PRPCMessage(greeting.__name__, (), {"name": "dasha", "part_of_day": "day"}), timedelta(seconds=3), "Hi dasha, have a nice day")
        ]
    )
    async def test_execution_task_without_error(self, func, message, timeout, result):
        worker = ThreadWorker(message, func, timeout)
        worker.start_work()

        await asyncio.wait([worker.get_future()], timeout=timeout.total_seconds())
        message_done = worker.get_message()

        assert message_done.result == result

    @pytest.mark.parametrize(
        "func, message, timeout",
        [
            (hello_world, PRPCMessage(hello_world.__name__, ("Hihi",), {}), timedelta(seconds=2)),
            (summ, PRPCMessage(summ.__name__, (5, None), {}), timedelta(seconds=2), ),
            (summ, PRPCMessage(summ.__name__, (5, 6), {"a": 5, "b": 6}), timedelta(seconds=2)),
            (greeting, PRPCMessage(greeting.__name__, ("Dasha",), {}), timedelta(seconds=2)),
            (func_time, PRPCMessage(greeting.__name__, (10,), {}), timedelta(seconds=2))
        ]
    )
    async def test_execution_task_with_error(self, func, message, timeout):
        worker = ThreadWorker(message, func, timeout)
        worker.start_work()

        await asyncio.wait([worker.get_future()], timeout=timeout.total_seconds())
        message_done = worker.get_message()

        assert message_done.exception_info is not None
        assert message_done.result is None
