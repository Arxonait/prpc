import asyncio
import concurrent.futures
import datetime
import logging
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import timedelta
from functools import partial
from typing import Literal, Callable

from main.task import Task, TaskDone

WORKER_TYPE = Literal["thread", "process", "async"]


class Worker(ABC):

    def __init__(self, task: Task, func: AbstractEventLoop, timeout: timedelta = None):
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
        if isinstance(self.task, TaskDone):
            return True

        if self._check_done_of_concurrence_obj():
            try:
                result = self._get_result_of_concurrence_obj()
            except Exception as e:
                self.task = TaskDone(**self.task.model_dump(), exception_info=str(e))
            else:
                self.task = TaskDone(**self.task.model_dump(), result=result)
            return True

        if self.timeout and datetime.datetime.now() - self._time_start_work > self.timeout:
            self.stop_work()
            self.task = TaskDone(**self.task.model_dump(),
                                 exception_info=f"the task was completed by server timeout {self.timeout.total_seconds()} secs.")
            return True

        return False

    def get_task(self):
        self.check_end_work()
        return self.task

    def get_result(self):
        if isinstance(self.task, TaskDone):
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
    worker: dict[WORKER_TYPE, Worker] = {
        "thread": ThreadWorker,
        "process": ProcessWorker,
        "async": AsyncWorker,
    }

    @classmethod
    def get_worker(cls, type_worker: WORKER_TYPE):
        worker_class = cls.worker.get(type_worker)

        if worker_class is None:
            raise Exception(f"only this workers: {cls.worker.keys()}")

        return worker_class


class WorkerManager:
    def __init__(self, max_number_worker: int | None, timeout_worker: timedelta | None = None):
        self.max_number_worker = max_number_worker
        self.timeout_worker = timeout_worker

        self.__current_workers: list[Worker] = []
        self.__end_workers: list[Worker] = []

    def __check_workers(self):
        copy_current_workers = self.__current_workers.copy()

        self.__current_workers = []
        for item in copy_current_workers:
            if item.check_end_work():
                self.__end_workers.append(item)
            else:
                self.__current_workers.append(item)

    def update_data_about_workers(self):
        self.__check_workers()

    def check_possibility_add_new_worker(self):
        self.__check_workers()
        if self.max_number_worker is not None and self.get_count_current_workers() >= self.max_number_worker:
            return False
        return True

    def get_count_current_workers(self):
        return len(self.__current_workers)

    def get_future_current_workers(self) -> list[asyncio.Future]:
        return [current_worker.get_future() for current_worker in self.__current_workers]

    def add_new_worker(self, task: Task, func, type_worker: str):
        if not self.check_possibility_add_new_worker():
            raise Exception("max workers")

        class_worker = WorkerFactory.get_worker(type_worker)
        new_worker = class_worker(task, func, self.timeout_worker)
        new_worker.start_work()
        self.__current_workers.append(new_worker)
        logging.debug(f"Задача task_id = {task.task_id} начала исполнятся воркером {type_worker}")

    @property
    def end_workers(self):
        return self.__end_workers

    def clear_end_workers(self):
        self.__end_workers.clear()
