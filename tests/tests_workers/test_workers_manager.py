import pytest

from main.func_module import FuncDataServer
from main.prpcmessage import PRPCMessage
from main.support_module.exceptions import NotFoundFunc
from main.workers.worker_manager import WorkerManager
from tests.data_for_tests import summ


@pytest.mark.parametrize(
    "message, is_raise_exception", [
        (PRPCMessage("summ", (), {}), False),
        (PRPCMessage("hello_world", (5, 6), {}), True),
    ]
)
def test_found_function_in_func_data(message, is_raise_exception):
    queue_stub = None
    func_data = [FuncDataServer(summ, "thread")]
    worker_manager = WorkerManager(queue_stub, func_data, None)
    try:
        worker_manager._get_func_data(message)
    except NotFoundFunc as e:
        assert is_raise_exception
    else:
        assert not is_raise_exception