import pytest

from main.func_module import FuncDataServer
from main.prpcmessage import PRPCMessage
from main.support_module.exceptions import NotFoundFunc
from main.workers.worker_manager import WorkerManager
from tests.data_for_tests import summ


def test_found_function_in_func_data():
    queue_stub = None
    func_data = [FuncDataServer(summ, "thread")]
    worker_manager = WorkerManager(queue_stub, func_data, None)
    worker_manager._get_func_data(PRPCMessage("summ", (), {}))


def test_not_found_function_in_func_data():
    queue_stub = None
    func_data = [FuncDataServer(summ, "thread")]
    worker_manager = WorkerManager(queue_stub, func_data, None)
    with pytest.raises(NotFoundFunc):
        worker_manager._get_func_data(PRPCMessage("hello_world", (5, 6), {}))