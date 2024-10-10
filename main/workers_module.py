import datetime
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import timedelta
from typing import Literal

from main.task import Task, TaskDone


class Worker(ABC):
    @abstractmethod
    def __init__(self, task: Task, func, timeout: timedelta):
        self.task = task
        self.timeout = timeout

        self.func = func

    @abstractmethod
    def start_work(self):
        raise NotImplementedError

    @abstractmethod
    def stop_work(self):
        raise NotImplementedError

    @abstractmethod
    def check_end_work(self):
        raise NotImplementedError

    @abstractmethod
    def get_result(self):
        raise NotImplementedError

    @abstractmethod
    def get_task(self):
        raise NotImplementedError


class ThreadWorker(Worker):
    executor = ThreadPoolExecutor()

    def __init__(self, task: Task, func, timeout: timedelta | None = None):
        super().__init__(task, func, timeout)
        self.thread: Future | None = None
        self.__time_start_work: datetime.datetime = None

    def start_work(self):
        self.thread = self.executor.submit(self.func, *self.task.func_args, **self.task.func_kwargs)
        self.__time_start_work = datetime.datetime.now()

    def check_end_work(self):
        if isinstance(self.task, TaskDone):
            return True

        if self.thread.done():
            try:
                result = self.thread.result()
            except Exception as e:
                self.task = TaskDone(**self.task.model_dump(), exception_info=str(e))
            else:
                self.task = TaskDone(**self.task.model_dump(), result=result)
            return True

        if self.timeout and datetime.datetime.now() - self.__time_start_work > self.timeout:
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

    def stop_work(self):
        self.thread.cancel()


class WorkerFactory:
    worker: dict[str, Worker] = {
        "thread": ThreadWorker
    }

    @classmethod
    def get_worker(cls, type_worker: str):
        worker_class = cls.worker.get(type_worker)

        if worker_class is None:
            raise Exception(f"only this workers: {cls.worker.keys()}")

        return worker_class


class WorkerManager:
    def __init__(self, type_worker: Literal["thread"],
                 max_number_worker: int | None,
                 timeout_worker: timedelta | None = None):

        self.class_worker = WorkerFactory.get_worker(type_worker)
        self.max_number_worker = max_number_worker
        self.timeout_worker = timeout_worker

        self.__current_workers: list[Worker] = []
        self.__end_workers: list[Worker] = []

    def __check_end_workers(self):
        for current_worker in self.__current_workers:
            if current_worker.check_end_work():
                self.__end_workers.append(current_worker)

        for end_workers in self.__end_workers:
            self.__current_workers.remove(end_workers)

    def check_add_new_worker(self):
        self.__check_end_workers()
        if self.max_number_worker is not None and self.get_count_current_workers() >= self.max_number_worker:
            return False
        return True

    def get_count_current_workers(self):
        return len(self.__current_workers)

    def add_new_worker(self, task: Task, func):
        if not self.check_add_new_worker():
            raise Exception("max workers")

        new_worker = self.class_worker(task, func, self.timeout_worker)
        new_worker.start_work()
        self.__current_workers.append(new_worker)

    @property
    def end_workers(self):
        return self.__end_workers

    def clear_end_workers(self):
        self.__end_workers.clear()
