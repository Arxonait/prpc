import asyncio
import datetime
from functools import wraps
from typing import get_args, Type

import pydantic

from prpc.brokers.brokers_factory import BrokerFactory, BROKER_ANNOTATION
from prpc.brokers import ServerBroker, AdminBroker
from prpc.func_data import FuncDataServer
from prpc.support_module.loggs import Logger
from prpc.workers import WORKER_TYPE_ANNOTATE, WorkerType
from prpc.workers.worker_manager import WorkerManager

logger = Logger.get_instance()
logger = logger.prpc_logger


def get_function_server() -> list[dict]:
    app_server: AppServer = AppServer.get_instance()
    func_data: list[FuncDataServer] = app_server.func_data
    return [func_data_item.serialize_data() for func_data_item in func_data
            if func_data_item.func not in app_server.system_func]


def ping():
    return "pong"


class InputDataAppServer(pydantic.BaseModel):
    type_broker: BROKER_ANNOTATION
    broker_url: str
    default_type_worker: WORKER_TYPE_ANNOTATE

    max_number_worker: int = pydantic.Field(ge=1, le=16)
    timeout_worker: datetime.timedelta | None
    name_queue: str
    group_name: str


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
                 broker_url: str,
                 default_type_worker: WORKER_TYPE_ANNOTATE = "thread",
                 max_number_worker: int = 4,
                 timeout_worker: datetime.timedelta | None = None,
                 name_queue="task_prpc",
                 *,
                 group_name="prpc_group_consumers"):
        """
        Приложение prpc\n
        Зарегистрированы функции:\n
        get_function_server() - создание файла 'server_func.py' для клиента\n
        ping() - проверка работы сервера\n

        :param type_broker: 'redis' or 'kafka'
        :param broker_url: 'redis://localhost:6379/0' or 'localhost:9092'
        :param default_type_worker: будет примениться ко всем зарегистрированным функциям, которые не имеют определенного worker. Типы workers: thread, process, async. Default value: 'thread'
        :param max_number_worker: кол-во workers которые могут одновременно работать и принимать новые сообщение. Относится ко типам workers. Default value: 4
        :param timeout_worker: максимальное время работы worker над одним сообщением. Type: datetime.timedelta | None. При None нет ограничений. Внимание в случае thread и process workers прервать выполнение самих потоков и процессов не получится из-за использования concurent.featutre (нет возмодности убить процесс и поток). Сам worker отправит результат (ошибка по timeout) и будет ожидать новое сообщение. Но эти потоки и процессы будут потреблять ресурсы pc. Обрабатывайте случаи, когда возможны бесконечные задачи.
        :param name_queue: Имя очереди. Default value: `task_prpc`
        :param group_name: Имя группы consumers. Default value: `prpc_group_consumers`
        """

        if self.__instance is not None:
            raise Exception("singleton cannot be instantiated more then once ")

        AppServer.__instance = self

        InputDataAppServer(type_broker=type_broker, broker_url=broker_url,
                           default_type_worker=default_type_worker, max_number_worker=max_number_worker,
                           timeout_worker=timeout_worker, name_queue=name_queue, group_name=group_name)

        self._type_broker = type_broker

        self._func_data: list[FuncDataServer] = []
        self.register_func(get_function_server, WorkerType.THREAD.value)
        self.register_func(ping, WorkerType.THREAD.value)

        self.default_type_worker = default_type_worker

        queue_class: ServerBroker = BrokerFactory.get_broker_class_server(type_broker)
        class_admin_broker = BrokerFactory.get_broker_class_admin(type_broker)
        self._admin_broker: AdminBroker = class_admin_broker(broker_url, name_queue, group_name=group_name)

        self.workers = []
        for i in range(max_number_worker):
            queue = queue_class(broker_url, name_queue, group_name)
            self.workers.append(WorkerManager(queue, self._func_data, timeout_worker))

        self._data_for_create_queues = {
            "number_of_workers": len(self.workers)
        }

    def _get_default_worker_type_or_target_worker_type(self, worker_type):
        return worker_type if worker_type else self.default_type_worker

    def decorator_reg_func(self, worker_type: WORKER_TYPE_ANNOTATE | None = None):
        """
        Регистрирует функцию в приложении
        :param worker_type: Может быть установлен None, тогда будет использован default_type_worker приложения. Типы workers: thread, process, async.
        """
        def decorator(func):

            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            register_func = func
            if self._get_default_worker_type_or_target_worker_type(worker_type) == "process":
                register_func = wrapper  # если для данной функции процесс, то регестрируем ее с декоратором для усипешного востановления процессом
            self.register_func(register_func, worker_type)

            return wrapper

        return decorator

    def register_func(self, func, worker_type: WORKER_TYPE_ANNOTATE | None = None):
        assert isinstance(worker_type, str | None), f"worker_type must be str or None"
        if not self._is_registered_func(func):
            worker_type = self._get_default_worker_type_or_target_worker_type(worker_type)
            assert worker_type in get_args(WORKER_TYPE_ANNOTATE), f"worker_type must be {get_args(WORKER_TYPE_ANNOTATE)}"

            self._func_data.append(FuncDataServer(func, worker_type))
            logger.info(f"Функция {func.__name__} зарегистрирована")

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
        logger.info("Старт сервера")
        await self._admin_broker.init(**self._data_for_create_queues)
        logger.info(f"Используется брокер {self._type_broker}")
        logger.info(f"{str(self._admin_broker.queue)}")
        logger.info(f"{str(self._admin_broker.queue_raw)}")

        tasks = []
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.start()))

        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    def start(self):
        asyncio.run(self.__start())
