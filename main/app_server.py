import asyncio
import datetime
import logging
from functools import wraps
from multiprocessing import freeze_support
from typing import Literal

from main.brokers_module import QueueFactory, AbstractQueueServer
from main.exceptions import NotFoundFunc
from main.func_module import FuncDataServer
from main.task import Task
from main.workers_module import WorkerManager, WORKER_TYPE_ANNOTATE, WorkerType


def get_function_server() -> list[dict]:
    func_data: list[FuncDataServer] = AppServer.get_instance().func_data
    return [func_data_item.serialize_data() for func_data_item in func_data]


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
                 default_type_worker: WORKER_TYPE_ANNOTATE = "thread",
                 max_number_worker: int = 4,
                 timeout_worker: datetime.timedelta | None = None,

                 name_queue="task_prpc",
                 expire_task_feedback=datetime.timedelta(hours=2),
                 expire_task_process=datetime.timedelta(hours=2)):

        if self.__instance is not None:
            raise Exception("singleton cannot be instantiated more then once ")

        AppServer.__instance = self

        self._func_data: list[FuncDataServer] = []
        self.register_func(get_function_server, WorkerType.THREAD.value)

        self.default_type_worker = default_type_worker
        self.worker_manager = WorkerManager(max_number_worker, timeout_worker)

        queue_class: AbstractQueueServer = QueueFactory.get_queue_class_server(type_broker)
        self.queue: AbstractQueueServer = queue_class(config_broker, name_queue, expire_task_feedback,
                                                      expire_task_process)

    def _get_default_worker_type_or_target_worker_type(self, worker_type):
        return worker_type if worker_type else self.default_type_worker

    def decorator_reg_func(self, worker_type: WORKER_TYPE_ANNOTATE | None = None):
        def decorator(func):
            self.register_func(func, worker_type)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def register_func(self, func, worker_type: WORKER_TYPE_ANNOTATE | None = None):
        if not self._is_registered_func(func):
            worker_type = self._get_default_worker_type_or_target_worker_type(worker_type)
            self._func_data.append(FuncDataServer(func, worker_type))
            logging.info(f"Функция {func.__name__} зарегистрирована")

    def register_funcs(self, *funcs, worker_type: WORKER_TYPE_ANNOTATE | None = None):
        for func in funcs:
            self.register_func(func, worker_type)

    @property
    def func_data(self):
        return self._func_data

    def __get_func_data(self, task: Task) -> FuncDataServer:
        for func_data in self._func_data:
            if task.func_name == func_data.func_name:
                return func_data
        raise NotFoundFunc(task.func_name)

    def _is_registered_func(self, func):
        for func_data in self._func_data:
            if func_data.func_name == func.__name__:
                return True
        return False

    async def _handler_task_with_exception(self, task: Task):
        assert task.exception_info is not None, "Только для задач с инофрмацией об ошибке"
        logging.warning(f"Задача {task} не выполнилась по причине {task.exception_info}")
        logging.debug(
            f"Свободные воркеры {self.worker_manager.max_number_worker - self.worker_manager.get_count_current_workers()}")
        await self.queue.add_task_in_feedback_queue(task)

    async def _task_get_new_task_from_queue(self):
        while True:
            if not self.worker_manager.check_possibility_add_new_worker():
                logging.debug(
                    f"Все воркеры заняты. Макс воркеров {self.worker_manager.max_number_worker} --- Текущие воркеры {self.worker_manager.get_count_current_workers()}")
                await asyncio.wait(self.worker_manager.get_future_current_workers(),
                                   return_when=asyncio.FIRST_COMPLETED)
                self.worker_manager.update_data_about_workers()

            logging.debug(f"Ожидание новой задачи")
            logging.debug(
                f"Свободные воркеры {self.worker_manager.max_number_worker - self.worker_manager.get_count_current_workers()}")
            task = await self.queue.get_next_task_from_queue()
            logging.info(f"Получена новая задача {task}")

            try:
                func_data = self.__get_func_data(task)
            except NotFoundFunc as e:
                task.task_to_done(exception_info=str(e))
                await self._handler_task_with_exception(task)
            else:
                self.worker_manager.add_new_worker(task, func_data.func, func_data.worker_type)

    async def _task_update_data_about_worker(self):
        while True:
            self.worker_manager.update_data_about_workers()

            for end_worker in self.worker_manager.end_workers:
                task_done = end_worker.get_task()
                logging.info(f"Задача {task_done} выполнилась")
                logging.debug(
                    f"Свободные воркеры {self.worker_manager.max_number_worker - self.worker_manager.get_count_current_workers()}")
                await self.queue.add_task_in_feedback_queue(task_done)
            else:
                self.worker_manager.clear_end_workers()

            await asyncio.sleep(1)

    async def __start(self):
        freeze_support()
        logging.info("Старт сервера")

        await self.queue.init()

        task_queue = asyncio.create_task(self._task_get_new_task_from_queue())
        task_update = asyncio.create_task(self._task_update_data_about_worker())

        await asyncio.wait([task_queue, task_update])

    def start(self):
        asyncio.run(self.__start())
