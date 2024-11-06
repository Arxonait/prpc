import uuid
from abc import ABC

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, KafkaClient, TopicPartition
from kafka.admin import NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError

from prpc.brokers import ServerBroker, ClientBroker, AdminBroker, AbstractBroker, AbstractQueue, AbstractQueueRaw
from prpc.settings_server import Settings
from prpc.support_module.loggs import Logger
from prpc.prpcmessage import PRPCMessage

logger = Logger.get_instance()
logger = logger.prpc_logger


class AbstractKafkaBroker(AbstractBroker, ABC):

    def _init_queue(self, queue_name):
        return KafkaQueue(queue_name)

    def _init_queue_raw(self, queue_name):
        return KafkaRawQueue(queue_name)


class KafkaAdminBroker(AdminBroker, AbstractKafkaBroker):
    async def init(self, number_of_workers: int, *args, **kwargs):
        number_of_partitions_main_topic = Settings.kafka_queue_topic_number_partitions()
        await self.create_topics(number_of_partitions_main_topic, number_of_workers)

    async def create_topics(self, number_of_partitions_topic: int, number_of_workers: int):
        assert number_of_partitions_topic is not None or number_of_workers is not None
        if number_of_partitions_topic is None:
            number_of_partitions_topic = number_of_workers

        admin_client = KafkaAdminClient(
            bootstrap_servers=self.broker_url,
            client_id=f'prpc_server_admin_client_{uuid.uuid4()}'
        )

        topics_name = []
        for queue in self.queues:
            topics_name.extend([queue.queue, queue.queue_feedback])

        feedback_topic_number_partitions = Settings.kafka_feedback_topic_number_partitions()
        replication_factor = Settings.kafka_replication_factor()
        topic_data = {}
        topic_data.update({
            queue.queue: number_of_partitions_topic for queue in self.queues
        })
        topic_data.update({
            queue.queue_feedback: feedback_topic_number_partitions for queue in self.queues
        })

        topic_list = [
            NewTopic(name=topic_data_key, num_partitions=topic_data[topic_data_key],
                     replication_factor=replication_factor)
            for topic_data_key in topic_data
        ]

        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Kafka: топики {topics_name} успешно созданы")
        except TopicAlreadyExistsError:

            topics_metadata = admin_client.describe_topics(topics_name)
            for topic_metadata in topics_metadata:
                current_partitions = topic_metadata["partitions"]
                num_existing_partitions = len(current_partitions)
                topics_name = topic_metadata["topic"]

                # Проверяем, нужно ли увеличивать количество партиций
                if num_existing_partitions < topic_data[topics_name]:
                    # Увеличиваем количество партиций
                    new_partitions = NewPartitions(total_count=topic_data[topics_name])
                    admin_client.create_partitions({topics_name: new_partitions})
                    logger.info(
                        f"Kafka: было увеличено колво партиций в топике `{topics_name}`, c {len(current_partitions)} до {topic_data[topics_name]}")


class KafkaServerBroker(ServerBroker, AbstractKafkaBroker):
    def __init__(self, config_broker: str, queue_name: str, group_name: str, **kwargs):
        super().__init__(config_broker, queue_name, group_name)
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    async def init(self):
        self.consumer = AIOKafkaConsumer(
            *(queue.queue for queue in self.queues),
            bootstrap_servers=self.broker_url,
            group_id=self._group_name,
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        self.producer = AIOKafkaProducer(bootstrap_servers=self.broker_url)
        await self.consumer.start()
        await self.producer.start()

    async def get_next_message_from_queue(self):
        msg = await self.consumer.getone()
        self._set_current_message(msg)
        queue = self._get_queue_for_message_queue_name(msg.topic)
        prpr_message = queue.convert_message_queue_to_prpc_message(msg)
        return prpr_message

    async def add_message_in_feedback_queue(self, message: PRPCMessage):
        await self.consumer.commit()
        message_queue: ConsumerRecord = self._get_current_message()
        queue = self._get_queue_for_message_queue_name(message_queue.topic)
        await self.producer.send_and_wait(queue.queue_feedback, queue.serialize_message_for_feedback(message).encode())


class KafkaClientBroker(ClientBroker, AbstractKafkaBroker):

    def __init__(self, config_broker: str, queue_name: str):
        super().__init__(config_broker, queue_name)
        self.producer = KafkaProducer(bootstrap_servers=self.broker_url)
        self._is_closed = False
        self.consumer = None

    def add_message_in_queue(self, message: PRPCMessage):
        assert not self._is_closed, "this client is closed"
        self.producer.send(self.queue.queue, message.serialize().encode())

    def _init_consumer_timestamp(self, message: PRPCMessage):
        if self.consumer is not None:
            return

        topic_name = self.queue.queue_feedback
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.broker_url,
            auto_offset_reset='latest',
            consumer_timeout_ms=1000
        )

        # Ожидаем назначения разделов
        while not consumer.assignment():
            consumer.poll(timeout_ms=100)  # Периодически вызываем poll для ожидания

        timestamp = int(message.date_create_message.timestamp() * 1000)

        partitions = consumer.partitions_for_topic(topic_name)
        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        offsets = consumer.offsets_for_times({tp: timestamp for tp in topic_partitions})

        for partition, offset in offsets.items():
            if offset is not None:
                consumer.seek(partition, offset.offset)

        self.consumer = consumer

    def search_message_in_feedback(self, target_message: PRPCMessage) -> PRPCMessage | None:
        assert not self._is_closed, "this client is closed"

        self._init_consumer_timestamp(target_message)

        try:
            for message in self.consumer:
                message = PRPCMessage.deserialize(message.value)
                if target_message.message_id == message.message_id:
                    return message
        except KafkaTimeoutError:
            return None

    def close(self):
        self._is_closed = True
        self.producer.close()
        if self.consumer is not None:
            self.consumer.close()


class KafkaQueue(AbstractQueue):
    def convert_message_queue_to_prpc_message(self, message) -> PRPCMessage:
        message = PRPCMessage.deserialize(message.value)
        return message


class KafkaRawQueue(AbstractQueueRaw, KafkaQueue):
    def convert_message_queue_to_prpc_message(self, message) -> PRPCMessage:
        message = PRPCMessage.deserialize_raw(message.value)
        return message

