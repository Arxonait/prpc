import logging


class Logger:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            Logger()
        return cls._instance

    def __init__(self):
        if self._instance is not None:
            raise Exception("singleton cannot be instantiated more then once ")

        Logger._instance = self

        self.prpc_logger = logging.getLogger("prpc")
        self.prpc_logger.setLevel(logging.DEBUG)
