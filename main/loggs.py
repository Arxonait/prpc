import logging


def get_logger():
    prpc_logger = logging.getLogger("prpc")
    prpc_logger.setLevel(logging.DEBUG)
    return prpc_logger
