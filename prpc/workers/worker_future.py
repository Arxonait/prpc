import asyncio
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
from functools import partial

from prpc.workers import Worker


class WorkerFuture(Worker, ABC):
    def __init__(self, message, func, timeout):
        super().__init__(message, func, timeout)
        self._concurrence_obj: asyncio.Future | None = None

    def get_future(self):
        return self._concurrence_obj

    def _get_result_of_concurrence_obj(self):
        return self._concurrence_obj.result()

    def _check_done_of_concurrence_obj(self):
        return self._concurrence_obj.done()

    def stop_work(self):
        self._concurrence_obj.cancel()


class ThreadWorker(WorkerFuture):
    executor = ThreadPoolExecutor()

    def start_work(self):
        self._time_start_work = datetime.now()
        self._concurrence_obj = (asyncio.get_running_loop().
                                 run_in_executor(self.executor,
                                                 partial(self.func, *self.message.func_args, **self.message.func_kwargs)))

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - thread worker can't work with coroutine")


class ProcessWorker(WorkerFuture):
    executor = ProcessPoolExecutor()

    def start_work(self):
        self._time_start_work = datetime.now()
        self._concurrence_obj = (asyncio.get_running_loop().
                                 run_in_executor(self.executor,
                                                 partial(self.func, *self.message.func_args, **self.message.func_kwargs)))

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - process worker can't work with coroutine")


class AsyncWorker(WorkerFuture):

    def start_work(self):
        self._time_start_work = datetime.now()
        task = asyncio.create_task(self.func(*self.message.func_args, **self.message.func_kwargs))
        self._concurrence_obj = task
        task.cancelling()

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if not func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - async worker can work only with coroutine")