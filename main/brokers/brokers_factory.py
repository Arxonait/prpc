from typing import Literal

from main.brokers import ServerBroker, ClientBroker
from main.brokers.kafka import KafkaServerBroker, KafkaClientBroker
from main.brokers.redis import RedisServerBroker, RedisClientBroker

BROKER_ANNOTATION = Literal["redis", "kafka"]


class BrokerFactory:
    server_queue: dict[str, ServerBroker] = {
        'redis': RedisServerBroker,
        'kafka': KafkaServerBroker,
    }

    sync_client_queue: dict[str, ClientBroker] = {
        'redis': RedisClientBroker,
        'kafka': KafkaClientBroker,
    }

    @classmethod
    def get_broker_class_server(cls, type_broker: BROKER_ANNOTATION) -> ServerBroker:
        queue_class = cls.server_queue.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.server_queue.keys()}")

        return queue_class

    @classmethod
    def get_broker_class_sync_client(cls, type_broker: BROKER_ANNOTATION) -> ClientBroker:
        queue_class = cls.sync_client_queue.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.sync_client_queue.keys()}")

        return queue_class
