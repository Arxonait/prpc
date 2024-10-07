import datetime
import logging
import time
from typing import Literal

from main.brokers_module import QueueWithFeedback, QueueWithFeedbackFactory
from main.exceptions import NotFoundFunc
from main.func_converter import FuncData
from main.task import Task, task_to_task_done
from main.workers_module import WorkerManager


class AppServer:
    def __init__(self,
                 type_broker: Literal["redis"],
                 config_broker: dict,
                 type_worker: Literal["thread"],
                 max_number_worker: int,

                 name_queue="task_prpc",
                 expire_task_feedback=datetime.timedelta(hours=12),
                 expire_task_process=datetime.timedelta(hours=12),):

        self._func_data: list[FuncData] = []

        self.worker_manager = WorkerManager(type_worker, max_number_worker)
        self.queue: QueueWithFeedback = QueueWithFeedbackFactory.get_queue(type_broker,
                                                                           config_broker,
                                                                           name_queue,
                                                                           expire_task_feedback,
                                                                           expire_task_process)

    def append_func(self, func):
        self._func_data.append(FuncData(func))

    def __get_func(self, task: Task):
        for func_data in self._func_data:
            if task.name_func == func_data.name_func:
                return func_data.func
        raise NotFoundFunc(task.name_func)

    def start(self):
        logging.info("Старт сервера")

        while True:
            if not self.worker_manager.check_add_new_worker():
                logging.debug(f"Все воркеры заняты. Макс воркеров {self.worker_manager.max_number_worker} --- Текущие воркеры {self.worker_manager.get_count_current_workers()}")
                time.sleep(1)
                continue

            for end_worker in self.worker_manager.end_workers:
                task_done = end_worker.get_task()
                logging.info(f"Задача {task_done.json()} выполнилась")
                self.queue.add_task_in_feedback(task_done)
            self.worker_manager.clear_end_workers()

            task = self.queue.get_next_task_in_queue()
            if task is None:
                logging.debug(f"Ожидание новой задачи, свободные воркеры {self.worker_manager.max_number_worker - self.worker_manager.get_count_current_workers()}")
                time.sleep(1)
                continue

            logging.info(f"Получена новая задача {task.json()}")

            try:
                func = self.__get_func(task)
            except NotFoundFunc as e:
                logging.warning(f"Задача {task.json()} не выполнилась по причине {str(e)}")
                self.queue.add_task_in_feedback(task_to_task_done(task, exception_info=str(e)))
                continue
            self.worker_manager.add_new_worker(task, func)




