import asyncio
import datetime
import logging
from functools import wraps
from multiprocessing import freeze_support
from typing import Literal

from main.brokers_module import QueueFactory, AbstractQueueServer
from main.func_module import FuncDataServer
from main.workers_module import WorkerManager, WORKER_TYPE_ANNOTATE, WorkerType


def get_function_server() -> list[dict]:
    app_server: AppServer = AppServer.get_instance()
    func_data: list[FuncDataServer] = app_server.func_data
    return [func_data_item.serialize_data() for func_data_item in func_data
            if func_data_item.func not in app_server.system_func]


def ping():
    return "pong"


class AppServer:
    __instance = None
    system_func = [get_function_server]

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
        self.register_func(ping, WorkerType.THREAD.value)

        self.default_type_worker = default_type_worker

        queue_class: AbstractQueueServer = QueueFactory.get_queue_class_server(type_broker)
        self.queue: AbstractQueueServer = queue_class(config_broker, name_queue, expire_task_feedback,
                                                      expire_task_process)

        self.workers = [WorkerManager(self.queue, self._func_data, timeout_worker) for _ in range(max_number_worker)]

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

    def _is_registered_func(self, func):
        for func_data in self._func_data:
            if func_data.func_name == func.__name__:
                return True
        return False

    async def __start(self):
        freeze_support()
        logging.info("Старт сервера")

        await self.queue.init()

        tasks = []
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.start()))

        await asyncio.wait(tasks)

    def start(self):
        asyncio.run(self.__start())
