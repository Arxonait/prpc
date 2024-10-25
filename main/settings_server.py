import datetime
import os


class Settings:
    _redis_expire_message_feedback: datetime.timedelta | None = None
    _redis_expire_message_process: datetime.timedelta | None = None

    @classmethod
    def redis_expire_task_feedback(cls):
        if cls._redis_expire_message_feedback is None:
            default_value = datetime.timedelta(hours=6).total_seconds()
            value = os.getenv("PRPC_REDIS_EXPIRE_MESSAGE_FEEDBACK_SEC", default_value)
            cls._redis_expire_message_feedback = datetime.timedelta(seconds=value)

        return cls._redis_expire_message_feedback

    @classmethod
    def redis_expire_task_process(cls):
        if cls._redis_expire_message_process is None:
            default_value = datetime.timedelta(days=5).total_seconds()
            value = os.getenv("PRPC_REDIS_EXPIRE_MESSAGE_PROCESS_SEC", default_value)
            cls._redis_expire_message_process = datetime.timedelta(seconds=value)

        return cls._redis_expire_message_process

