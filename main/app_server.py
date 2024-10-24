import asyncio
import datetime
from functools import wraps
from multiprocessing import freeze_support

import pydantic

from main.brokers.brokers_factory import BrokerFactory, BROKER_ANNOTATION
from main.brokers import ServerBroker, AdminBroker
from main.func_module import FuncDataServer
from main.loggs import get_logger
from main.workers_module import WorkerManager, WORKER_TYPE_ANNOTATE, WorkerType


def get_function_server() -> list[dict]:
    app_server: AppServer = AppServer.get_instance()
    func_data: list[FuncDataServer] = app_server.func_data
    return [func_data_item.serialize_data() for func_data_item in func_data
            if func_data_item.func not in app_server.system_func]


def ping():
    return "pong"


class InputDataAppServer(pydantic.BaseModel):
    type_broker: BROKER_ANNOTATION
    config_broker: dict | str
    default_type_worker: WORKER_TYPE_ANNOTATE

    max_number_worker: int = pydantic.Field(ge=1, le=16)
    timeout_worker: datetime.timedelta | None
    name_queue: str
    kafka_number_of_partitions_main_topic: int | None


class AppServer:
    __instance = None
    system_func = [get_function_server]

    @classmethod
    def get_instance(cls):
        if cls.__instance is None:
            raise Exception("Initialize the class `AppServer`")
        return cls.__instance

    def __init__(self,
                 type_broker: BROKER_ANNOTATION,
                 config_broker: dict | str,
                 default_type_worker: WORKER_TYPE_ANNOTATE = "thread",
                 max_number_worker: int = 4,
                 timeout_worker: datetime.timedelta | None = None,

                 name_queue="task_prpc",
                 *args,
                 kafka_number_of_partitions_main_topic: int | None = None):

        if self.__instance is not None:
            raise Exception("singleton cannot be instantiated more then once ")

        AppServer.__instance = self

        InputDataAppServer(type_broker=type_broker, config_broker=config_broker,
                           default_type_worker=default_type_worker, max_number_worker=max_number_worker,
                           timeout_worker=timeout_worker, name_queue=name_queue,
                           kafka_number_of_partitions_main_topic=kafka_number_of_partitions_main_topic)

        self._func_data: list[FuncDataServer] = []
        self.register_func(get_function_server, WorkerType.THREAD.value)
        self.register_func(ping, WorkerType.THREAD.value)

        self.default_type_worker = default_type_worker

        queue_class: ServerBroker = BrokerFactory.get_broker_class_server(type_broker)
        class_admin_broker = BrokerFactory.get_broker_class_admin(type_broker)
        self._admin_broker: AdminBroker = class_admin_broker(config_broker, name_queue)

        self.workers = []
        for _ in range(max_number_worker):
            queue = queue_class(config_broker, name_queue)
            self.workers.append(WorkerManager(queue, self._func_data, timeout_worker))

        self._data_for_create_queues = {
            "number_of_partitions_main_topic": kafka_number_of_partitions_main_topic,
            "number_of_workers": len(self.workers)
        }

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
            get_logger().info(f"Функция {func.__name__} зарегистрирована")

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
        freeze_support() # todo
        get_logger().info("Старт сервера")
        await self._admin_broker.create_queues(**self._data_for_create_queues)

        tasks = []
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.start()))

        await asyncio.wait(tasks)

    def start(self):
        asyncio.run(self.__start())
