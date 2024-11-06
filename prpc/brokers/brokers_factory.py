from typing import Literal

from prpc.brokers import ServerBroker, ClientBroker, AdminBroker
from prpc.brokers.kafka import KafkaServerBroker, KafkaClientBroker, KafkaAdminBroker
from prpc.brokers.redis import RedisServerBroker, RedisClientBroker, RedisAdminBroker

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

    admin_broker: dict[BROKER_ANNOTATION, AdminBroker] = {
        "redis": RedisAdminBroker,
        "kafka": KafkaAdminBroker
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

    @classmethod
    def get_broker_class_admin(cls, type_broker: BROKER_ANNOTATION) -> AdminBroker:
        queue_class = cls.admin_broker.get(type_broker)

        if queue_class is None:
            raise Exception(f"only this broker: {cls.admin_broker.keys()}")

        return queue_class
