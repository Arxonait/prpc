import datetime
import logging
import time
from typing import Literal

from main.brokers_module import QueueWithFeedback, QueueWithFeedbackFactory
from main.exceptions import NotFoundFunc
from main.func_converter import FuncData
from main.task import Task, task_to_task_done
from main.workers_module import WorkerManager


def get_function_server() -> list[str]:
    func_data = AppServer.get_instance().func_data
    return [func_data_item.create_func() for func_data_item in func_data]


class AppServer:
    __instance = None

    @classmethod
    def get_instance(cls):
        if cls.__instance is None:
            raise Exception("init app server")
        return cls.__instance

    def __init__(self,
                 type_broker: Literal["redis"],
                 config_broker: dict | str,
                 type_worker: Literal["thread"],
                 max_number_worker: int,
                 timeout_worker: datetime.timedelta | None = None,

                 name_queue="task_prpc",
                 expire_task_feedback=datetime.timedelta(hours=12),
                 expire_task_process=datetime.timedelta(hours=12)):

        if self.__instance is not None:
            raise Exception("singleton cannot be instantiated more then once ")

        AppServer.__instance = self

        self._func_data: list[FuncData] = []
        self.register_func(get_function_server)

        self.worker_manager = WorkerManager(type_worker, max_number_worker, timeout_worker)
        queue_class: QueueWithFeedback = QueueWithFeedbackFactory.get_queue_class(type_broker)
        self.queue = queue_class(config_broker, name_queue, expire_task_feedback, expire_task_process)

    def register_func(self, func):
        self._func_data.append(FuncData(func))

    def register_funcs(self, *funcs):
        self._func_data.extend(map(FuncData, funcs))

    @property
    def func_data(self):
        return self._func_data

    def __get_func(self, task: Task):
        for func_data in self._func_data:
            if task.name_func == func_data.func_name:
                return func_data.func
        raise NotFoundFunc(task.name_func)

    def start(self):
        logging.info("Старт сервера")

        while True:
            if not self.worker_manager.check_add_new_worker():
                logging.debug(
                    f"Все воркеры заняты. Макс воркеров {self.worker_manager.max_number_worker} --- Текущие воркеры {self.worker_manager.get_count_current_workers()}")
                time.sleep(1)  # todo
                continue

            for end_worker in self.worker_manager.end_workers:
                task_done = end_worker.get_task()
                logging.info(f"Задача {task_done.json()} выполнилась")
                self.queue.add_task_in_feedback(task_done)
            self.worker_manager.clear_end_workers()

            task = self.queue.get_next_task_in_queue()
            if task is None:
                logging.debug(
                    f"Ожидание новой задачи, свободные воркеры {self.worker_manager.max_number_worker - self.worker_manager.get_count_current_workers()}")
                time.sleep(1)  # todo
                continue

            logging.info(f"Получена новая задача {task.json()}")

            try:
                func = self.__get_func(task)
            except NotFoundFunc as e:
                logging.warning(f"Задача {task.json()} не выполнилась по причине {str(e)}")
                self.queue.add_task_in_feedback(task_to_task_done(task, exception_info=str(e)))
                continue
            self.worker_manager.add_new_worker(task, func)
