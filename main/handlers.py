import functools

from main.loggs import Logger

logger = Logger.get_instance()
logger = logger.prpc_logger


def handler_errors(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.critical(str(e))
            raise e

    return wrapper
