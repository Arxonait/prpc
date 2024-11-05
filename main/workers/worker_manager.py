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
                message = await self.queue.get_next_message_from_queue()
            except (PRPCMessageDeserializeError, MessageFromStreamDataValidationError, JSONDeserializeError) as e:
                logger.warning(str(e))  # todo отправлять овтет об ошибках?
                continue

            logger.info(f"Получена новая задача {message}")

            try:
                func_data = self._get_func_data(message)
            except NotFoundFunc as e:
                message.message_to_done(exception_info=str(e))
                await self._handler_task_with_exception(message)
            else:
                self.create_current_worker(message, func_data.func, func_data.worker_type)

                tm = self.timeout_worker.total_seconds() if self.timeout_worker else None
                await asyncio.wait([self.current_worker.get_future()], timeout=tm)  # study!!!
                message_done = self.current_worker.get_message()

                logger.info(f"Задача {message_done} выполнилась")
                await self.queue.add_message_in_feedback_queue(message_done)
            finally:
                self.current_worker = None

    def create_current_worker(self, message: PRPCMessage, func: Callable, type_worker: WORKER_TYPE_ANNOTATE):
        assert self.current_worker is None

        class_worker = WorkerFactory.get_worker(type_worker)
        self.current_worker = class_worker(message, func, self.timeout_worker)
        self.current_worker.start_work()
        logger.debug(f"Задача task_id = {message.message_id} начала исполнятся воркером {type_worker}")

    def _get_func_data(self, message: PRPCMessage):
        for func_data in self.func_data:
            if message.func_name == func_data.func_name:
                return func_data
        raise NotFoundFunc(message.func_name)

    async def _handler_task_with_exception(self, message: PRPCMessage):
        assert message.exception_info is not None, "Только для задач с инофрмацией об ошибке"
        logger.warning(f"Задача {message} не выполнилась по причине {message.exception_info}")
        await self.queue.add_message_in_feedback_queue(message)
