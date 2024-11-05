import datetime
import os
import uuid

from main.support_module.loggs import Logger

logger = Logger.get_instance()
logger = logger.prpc_logger


class Settings:
    _redis_expire_message_feedback: datetime.timedelta | None = None
    _redis_heartbeat_interval: int | None = None
    _redis_recover_interval: int | None = None

    _instance_name: str | None = None

    _kafka_feedback_topic_number_partitions: int | None = None
    _kafka_replication_factor: int | None = None

    @classmethod
    def redis_expire_task_feedback(cls):
        if cls._redis_expire_message_feedback is None:
            default_value = datetime.timedelta(hours=6).total_seconds()
            value = os.getenv("PRPC_REDIS_EXPIRE_MESSAGE_FEEDBACK_SEC", default_value)
            cls._redis_expire_message_feedback = datetime.timedelta(seconds=value)

        return cls._redis_expire_message_feedback

    @classmethod
    def redis_heartbeat(cls):
        if cls._redis_heartbeat_interval is None:
            default_value = 15
            value = os.getenv("PRPC_REDIS_HEARTBEAT_INTERVAL_SEC", default_value)
            cls._redis_heartbeat_interval = int(value)

        return cls._redis_heartbeat_interval

    @classmethod
    def redis_recover_interval(cls):
        if cls._redis_recover_interval is None:
            default_value = 45
            value = os.getenv("PRPC_REDIS_RECOVER_INTERVAL_SEC", default_value)
            cls._redis_recover_interval = int(value)

        return cls._redis_recover_interval

    @classmethod
    def instance_name(cls):
        if cls._instance_name is None:
            default_value = str(uuid.uuid4())
            cls._instance_name = os.getenv("PRPC_INSTANCE_NAME", default_value)  # todo save and parse, generate
            logger.info(f"PRPC instance name `{cls._instance_name}`")

        return cls._instance_name

    @classmethod
    def kafka_feedback_topic_number_partitions(cls):
        if cls._kafka_feedback_topic_number_partitions is None:
            default_value = 4
            value = os.getenv("PRPC_KAFKA_FEEDBACK_TOPIC_NUMBER_PARTITIONS", default_value)
            cls._kafka_feedback_topic_number_partitions = int(value)
        return cls._kafka_feedback_topic_number_partitions

    @classmethod
    def kafka_replication_factor(cls):
        if cls._kafka_replication_factor is None:
            default_value = 1
            value = os.getenv("PRPC_KAFKA_REPLICATION_FACTOR", default_value)
            cls._kafka_replication_factor = int(value)
        return cls._kafka_replication_factor

