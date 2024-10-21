import asyncio
import datetime
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import timedelta
from enum import Enum
from functools import partial
from typing import Literal, Callable

from main.brokers_module import AbstractQueueServer
from main.exceptions import NotFoundFunc
from main.task import Task


class WorkerType(Enum):
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"


WORKER_TYPE_ANNOTATE = Literal["thread", "process", "async"]


class Worker(ABC):

    def __init__(self, task: Task, func: Callable, timeout: timedelta = None):
        self.task = task
        self.timeout = timeout
        self.func = func

        self._time_start_work: datetime.datetime | None = None

    @abstractmethod
    def get_future(self) -> asyncio.Future | asyncio.Task:
        raise NotImplementedError

    @abstractmethod
    def start_work(self):
        raise NotImplementedError

    @abstractmethod
    def stop_work(self):
        raise NotImplementedError

    @abstractmethod
    def _check_done_of_concurrence_obj(self):
        raise NotImplementedError

    @abstractmethod
    def _get_result_of_concurrence_obj(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def check_ability_to_work_with_function(cls, func_data):
        raise NotImplementedError

    def check_end_work(self):
        if self.task.is_task_done():
            return True

        if self._check_done_of_concurrence_obj():
            try:
                result = self._get_result_of_concurrence_obj()
            except Exception as e:
                self.task.task_to_done(exception_info=str(e))
            else:
                self.task.task_to_done(result=result)
            return True

        if self.timeout and datetime.datetime.now() - self._time_start_work > self.timeout:
            self.stop_work()
            self.task.task_to_done(exception_info=f"the task was completed by server timeout {self.timeout.total_seconds()} secs.")
            return True

        return False

    def get_task(self):
        self.check_end_work()
        return self.task

    def get_result(self):
        if self.task.is_task_done():
            return self.task.result
        return None


class WorkerFuture(Worker, ABC):
    def __init__(self, task, func, timeout):
        super().__init__(task, func, timeout)
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
        self._time_start_work = datetime.datetime.now()
        self._concurrence_obj = (asyncio.get_running_loop().
                                 run_in_executor(self.executor,
                                                 partial(self.func, *self.task.func_args, **self.task.func_kwargs)))

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - thread worker can't work with coroutine")


class ProcessWorker(WorkerFuture):
    executor = ProcessPoolExecutor()

    def start_work(self):
        self._time_start_work = datetime.datetime.now()
        self._concurrence_obj = (asyncio.get_running_loop().
                                 run_in_executor(self.executor,
                                                 partial(self.func, *self.task.func_args, **self.task.func_kwargs)))

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - process worker can't work with coroutine")


class AsyncWorker(WorkerFuture):

    def start_work(self):
        self._time_start_work = datetime.datetime.now()
        task = asyncio.create_task(self.func(*self.task.func_args, **self.task.func_kwargs))
        self._concurrence_obj = task
        task.cancelling()

    @classmethod
    def check_ability_to_work_with_function(cls, func_data):
        if not func_data.is_coroutine:
            raise Exception(f"{func_data.func_name} - async worker can work only with coroutine")


class WorkerFactory:
    worker: dict[WORKER_TYPE_ANNOTATE, Worker] = {
        "thread": ThreadWorker,
        "process": ProcessWorker,
        "async": AsyncWorker,
    }

    @classmethod
    def get_worker(cls, type_worker: WORKER_TYPE_ANNOTATE):
        worker_class = cls.worker.get(type_worker)

        if worker_class is None:
            raise Exception(f"only this workers: {cls.worker.keys()}")

        return worker_class


class WorkerManager:
    def __init__(self, queue: AbstractQueueServer,
                 func_data: list,
                 timeout_worker: timedelta | None = None):

        self.queue = queue
        self.func_data = func_data
        self.timeout_worker = timeout_worker
        self.current_worker: Worker | None = None

    async def start(self):
        while True:
            task = await self.queue.get_next_task_from_queue()
            logging.info(f"Получена новая задача {task}")

            try:
                func_data = self._get_func_data(task)
            except NotFoundFunc as e:
                task.task_to_done(exception_info=str(e))
                await self._handler_task_with_exception(task)
            else:
                self.create_current_worker(task, func_data.func, func_data.worker_type)

                tm = self.timeout_worker.total_seconds() if self.timeout_worker else None
                await asyncio.wait([self.current_worker.get_future()], timeout=tm)
                task_done = self.current_worker.get_task()

                logging.info(f"Задача {task_done} выполнилась")
                await self.queue.add_task_in_feedback_queue(task_done)
            finally:
                self.current_worker = None

    def create_current_worker(self, task: Task, func: Callable, type_worker: WORKER_TYPE_ANNOTATE):
        assert self.current_worker is None

        class_worker = WorkerFactory.get_worker(type_worker)
        self.current_worker = class_worker(task, func, self.timeout_worker)
        self.current_worker.start_work()
        logging.debug(f"Задача task_id = {task.task_id} начала исполнятся воркером {type_worker}")

    def _get_func_data(self, task: Task):
        for func_data in self.func_data:
            if task.func_name == func_data.func_name:
                return func_data
        raise NotFoundFunc(task.func_name)

    async def _handler_task_with_exception(self, task: Task):
        assert task.exception_info is not None, "Только для задач с инофрмацией об ошибке"
        logging.warning(f"Задача {task} не выполнилась по причине {task.exception_info}")
        await self.queue.add_task_in_feedback_queue(task)
