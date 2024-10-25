from main.workers import Worker, WORKER_TYPE_ANNOTATE
from main.workers.worker_future import ThreadWorker, ProcessWorker, AsyncWorker


class WorkerFactory:
    worker: dict[WORKER_TYPE_ANNOTATE, Worker] = {
        "thread": ThreadWorker,
        "process": ProcessWorker,
        "async": AsyncWorker,
    }

    @classmethod
    def get_worker(cls, type_worker: WORKER_TYPE_ANNOTATE):
        worker_class = cls.worker.get(type_worker)

        if worker_class is None:
            raise Exception(f"only this workers: {cls.worker.keys()}")

        return worker_class
