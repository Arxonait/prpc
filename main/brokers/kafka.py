import logging
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError

from main.brokers import ServerBroker, ClientBroker
from main.task import Task


class ManagerCashKafkaClientBroker:
    task_in_process: list[uuid.UUID] = []  # todo система кеширования
    cash_tasks: list[Task] = []

    @classmethod
    def tasks_in_process_append(cls, task_id):
        cls.task_in_process.append(task_id)

    @classmethod
    def tasks_in_process_remove(cls, task_id):
        try:
            cls.task_in_process.remove(task_id)
        except ValueError as e:
            logging.warning("")  # todo

    @classmethod
    def get_task_from_cash(cls, task: Task):
        result_search: Task | None = None
        for cashed_task in cls.cash_tasks:
            if cashed_task.task_id == task.task_id:
                result_search = cashed_task
                break

        if result_search:
            cls.cash_tasks.remove(result_search)

        return result_search


class KafkaServerBroker(ServerBroker):
    def __init__(self, config_broker: str | dict, queue_name: str, *args, **kwargs):
        super().__init__(config_broker, queue_name, args, kwargs)
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    async def init(self):
        self.consumer = AIOKafkaConsumer(
            self.get_queue_name(),
            bootstrap_servers=self.config_broker, # todo формат url
            group_id="prpc_group",
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.config_broker)
        await self.consumer.start()
        await self.producer.start()

    async def create_queues(self, number_of_workers: int):
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.config_broker,
            client_id='server_client'
        )

        topic_data = {
            self.get_queue_name(): number_of_workers + 2,
            self.get_queue_feedback_name(): 4
        }

        # Создание топика
        topic_list = [
            NewTopic(name=self.get_queue_name(), num_partitions=topic_data[self.get_queue_name()],
                     replication_factor=1),
            NewTopic(name=self.get_queue_feedback_name(), num_partitions=topic_data[self.get_queue_feedback_name()],
                     replication_factor=1)
        ]

        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError:

            topics_metadata = admin_client.describe_topics([self.get_queue_name(), self.get_queue_feedback_name()])
            for topic_metadata in topics_metadata:
                current_partitions = topic_metadata["partitions"]
                num_existing_partitions = len(current_partitions)
                topic_name = topic_metadata["topic"]

                # Проверяем, нужно ли увеличивать количество партиций
                if num_existing_partitions < topic_data[topic_name]:
                    # Увеличиваем количество партиций
                    new_partitions = NewPartitions(total_count=topic_data[topic_name])
                    admin_client.create_partitions({topic_name: new_partitions})

    async def get_next_task_from_queue(self):
        msg = await self.consumer.getone()
        task = Task.deserialize(msg.value)
        return task

    async def add_task_in_feedback_queue(self, task: Task):
        await self.consumer.commit()
        await self.producer.send_and_wait(self.get_queue_feedback_name(), task.serialize().encode())


class KafkaClientBroker(ClientBroker):
    manager_cash = ManagerCashKafkaClientBroker()

    def __init__(self, config_broker: str | dict, queue_name: str, *args, **kwargs):
        super().__init__(config_broker, queue_name, args, kwargs)
        self.consumer = KafkaConsumer(
            self.get_queue_feedback_name(),
            bootstrap_servers=self.config_broker,
            auto_offset_reset='latest',
            consumer_timeout_ms=1000
        )

        # Ожидаем назначения разделов
        while not self.consumer.assignment():
            self.consumer.poll(timeout_ms=100)  # Периодически вызываем poll для ожидания

        self.producer = KafkaProducer(bootstrap_servers=self.config_broker)

    def add_task_in_queue(self, task: Task):
        self.producer.send(self.get_queue_name(), task.serialize().encode())
        self.manager_cash.tasks_in_process_append(task.task_id)

    def search_task_in_feedback(self, searched_task: Task) -> Task | None:
        cashed_task = self.manager_cash.get_task_from_cash(searched_task)
        if cashed_task:
            assert searched_task.task_id == cashed_task.task_id
            return cashed_task

        try:
            for message in self.consumer:
                task = Task.deserialize(message.value)
                if searched_task.task_id == task.task_id:
                    self.manager_cash.tasks_in_process_remove(task.task_id)
                    assert task.task_id == searched_task.task_id
                    return task
                if task.task_id in self.manager_cash.task_in_process:
                    self.manager_cash.tasks_in_process_remove(task.task_id)
                    self.manager_cash.cash_tasks.append(task)
        except KafkaTimeoutError:
            return None
