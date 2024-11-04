import asyncio
from datetime import timedelta
from typing import Callable

from main.brokers import ServerBroker
from main.support_module.exceptions import NotFoundFunc, PRPCMessageDeserializeError, \
    MessageFromStreamDataValidationError, JSONDeserializeError
from main.support_module.handlers import handler_errors
from main.support_module.loggs import Logger
from main.prpcmessage import PRPCMessage
from main.workers import WORKER_TYPE_ANNOTATE, Worker
from main.workers.workers_factory import WorkerFactory

logger = Logger.get_instance()
logger = logger.prpc_logger


class WorkerManager:
    def __init__(self, queue: ServerBroker,
                 func_data: list,
                 timeout_worker: timedelta | None = None):

        self.queue = queue
        self.func_data = func_data
        self.timeout_worker = timeout_worker
        self.current_worker: Worker | None = None

    @handler_errors
    async def start(self):
        await self.queue.init()
        while True:
            try:
                task = await self.queue.get_next_message_from_queue()
            except (PRPCMessageDeserializeError, MessageFromStreamDataValidationError, JSONDeserializeError) as e:
                logger.warning(str(e))  # todo отправлять овтет об ошибках?
                continue

            logger.info(f"Получена новая задача {task}")

            try:
                func_data = self._get_func_data(task)
            except NotFoundFunc as e:
                task.message_to_done(exception_info=str(e))
                await self._handler_task_with_exception(task)
            else:
                self.create_current_worker(task, func_data.func, func_data.worker_type)

                tm = self.timeout_worker.total_seconds() if self.timeout_worker else None
                await asyncio.wait([self.current_worker.get_future()], timeout=tm)  # study!!!
                task_done = self.current_worker.get_message()

                logger.info(f"Задача {task_done} выполнилась")
                await self.queue.add_message_in_feedback_queue(task_done)
            finally:
                self.current_worker = None

    def create_current_worker(self, task: PRPCMessage, func: Callable, type_worker: WORKER_TYPE_ANNOTATE):
        assert self.current_worker is None

        class_worker = WorkerFactory.get_worker(type_worker)
        self.current_worker = class_worker(task, func, self.timeout_worker)
        self.current_worker.start_work()
        logger.debug(f"Задача task_id = {task.message_id} начала исполнятся воркером {type_worker}")

    def _get_func_data(self, task: PRPCMessage):
        for func_data in self.func_data:
            if task.func_name == func_data.func_name:
                return func_data
        raise NotFoundFunc(task.func_name)

    async def _handler_task_with_exception(self, task: PRPCMessage):
        assert task.exception_info is not None, "Только для задач с инофрмацией об ошибке"
        logger.warning(f"Задача {task} не выполнилась по причине {task.exception_info}")
        await self.queue.add_message_in_feedback_queue(task)
