import functools

from main import loggs


def handler_errors(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            loggs.get_logger().critical(str(e))
            raise e

    return wrapper
