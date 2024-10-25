import logging
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError

from main import loggs
from main.brokers import ServerBroker, ClientBroker, AdminBroker
from main.prpcmessage import PRPCMessage


class ManagerCashKafkaClientBroker:
    task_in_process: list[uuid.UUID] = []  # todo система кеширования
    cash_tasks: list[PRPCMessage] = []

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
    def get_task_from_cash(cls, task: PRPCMessage):
        result_search: PRPCMessage | None = None
        for cashed_task in cls.cash_tasks:
            if cashed_task.message_id == task.message_id:
                result_search = cashed_task
                break

        if result_search:
            cls.cash_tasks.remove(result_search)

        return result_search


class KafkaAdminBroker(AdminBroker):
    async def init(self, number_of_partitions_main_topic: int, number_of_workers: int, *args, **kwargs):
        await self.create_queues(number_of_partitions_main_topic, number_of_workers)

    async def create_queues(self, number_of_partitions_main_topic: int, number_of_workers: int):
        assert number_of_partitions_main_topic is not None or number_of_workers is not None
        if number_of_partitions_main_topic is None:
            number_of_partitions_main_topic = number_of_workers + 2

        admin_client = KafkaAdminClient(
            bootstrap_servers=self.broker_url,
            client_id='server_client'
        )

        topic_data = {
            self.get_queue_name(): number_of_partitions_main_topic,
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
            loggs.get_logger().info(f"Kafka: топики `{self.get_queue_name()}`, `{self.get_queue_feedback_name()}` успешно созданы")
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
                    loggs.get_logger().info(
                        f"Kafka: было увеличено колво партиций в топике `{topic_name}`, c {len(current_partitions)} до {topic_data[topic_name]}")


class KafkaServerBroker(ServerBroker):
    def __init__(self, config_broker: str | dict, queue_name: str, *args, **kwargs):
        super().__init__(config_broker, queue_name, args, kwargs)
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    async def init(self):
        self.consumer = AIOKafkaConsumer(
            self.get_queue_name(),
            bootstrap_servers=self.broker_url, # todo формат url
            group_id="prpc_group",
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.broker_url)
        await self.consumer.start()
        await self.producer.start()

    async def get_next_message_from_queue(self):
        msg = await self.consumer.getone()
        message = PRPCMessage.deserialize(msg.value)
        return message

    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        await self.consumer.commit()
        await self.producer.send_and_wait(self.get_queue_feedback_name(), message.serialize().encode())


class KafkaClientBroker(ClientBroker):
    manager_cash = ManagerCashKafkaClientBroker()

    def __init__(self, config_broker: str | dict, queue_name: str, *args, **kwargs):
        super().__init__(config_broker, queue_name, args, kwargs)
        self.consumer = KafkaConsumer(
            self.get_queue_feedback_name(),
            bootstrap_servers=self.broker_url,
            auto_offset_reset='latest',
            consumer_timeout_ms=1000
        )

        # Ожидаем назначения разделов
        while not self.consumer.assignment():
            self.consumer.poll(timeout_ms=100)  # Периодически вызываем poll для ожидания

        self.producer = KafkaProducer(bootstrap_servers=self.broker_url)

    def add_message_in_queue(self, message: PRPCMessage):
        self.producer.send(self.get_queue_name(), message.serialize().encode())
        self.manager_cash.tasks_in_process_append(message.message_id)

    def search_message_in_feedback(self, message: PRPCMessage) -> PRPCMessage | None:
        cashed_task = self.manager_cash.get_task_from_cash(message)
        if cashed_task:
            assert message.message_id == cashed_task.message_id
            return cashed_task

        try:
            for message in self.consumer:
                task = PRPCMessage.deserialize(message.value)
                if message.message_id == task.message_id:
                    self.manager_cash.tasks_in_process_remove(task.message_id)
                    assert task.message_id == message.message_id
                    return task
                if task.message_id in self.manager_cash.task_in_process:
                    self.manager_cash.tasks_in_process_remove(task.message_id)
                    self.manager_cash.cash_tasks.append(task)
        except KafkaTimeoutError:
            return None
