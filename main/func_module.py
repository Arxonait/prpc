import inspect
from typing import Callable

from main.support_module.loggs import Logger
from main.type_module import HandlerAnnotation
from main.workers import WORKER_TYPE_ANNOTATE
from main.workers.workers_factory import WorkerFactory

logger = Logger.get_instance()
logger = logger.prpc_logger


class FuncDataServer:
    def __init__(self, func: Callable, worker_type: WORKER_TYPE_ANNOTATE):
        self.func = func
        self.worker_type = worker_type

        self.func_args = dict[str, inspect.Parameter]
        self.func_kwargs = dict[str, inspect.Parameter]
        self.func_name: str = None
        self.is_coroutine: bool = None
        self._parse_func_data()
        self._validate_func_data()

    def _parse_func_data(self):
        # todo parse dock string
        self.func_name = self.func.__name__

        # Сигнатура функции с аргументами
        func_signature = inspect.signature(self.func)

        # Список аргументов
        func_args = func_signature.parameters
        self.func_args = dict(filter(lambda item: item[1].default == inspect.Parameter.empty, func_args.items()))
        self.func_kwargs = dict(filter(lambda item: item[1].default != inspect.Parameter.empty, func_args.items()))
        self.is_coroutine = inspect.iscoroutinefunction(self.func)

    def serialize_data(self) -> dict:
        data = {
            "func_name": self.func_name,
            "func_args": [self._serialize_func_parameter(parameter) for parameter in self.func_args.values()],
            "func_kwargs": [self._serialize_func_parameter(parameter) for parameter in self.func_kwargs.values()]
        }
        return data

    def _serialize_func_parameter(self, parameter: inspect.Parameter) -> dict:
        data = {"name": parameter.name}
        if parameter.default != inspect.Parameter.empty:
            data["default"] = parameter.default
        if parameter.annotation != inspect.Parameter.empty:
            data["annotation"] = HandlerAnnotation.serialize_annotation(parameter.annotation)
        return data

    def _validate_func_data(self):
        worker_class = WorkerFactory.get_worker(self.worker_type)
        worker_class.check_ability_to_work_with_function(self)

        invalid_annotations = []
        for func_arg in self.func_args.values():
            if func_arg.annotation != inspect.Parameter.empty:
                invalid_annotations.extend(HandlerAnnotation.is_valid_annotation(func_arg.annotation))

        for func_arg in self.func_kwargs.values():
            if func_arg.annotation != inspect.Parameter.empty:
                invalid_annotations.extend(HandlerAnnotation.is_valid_annotation(func_arg.annotation))

        if invalid_annotations:
            logger.warning(f"Функциия '{self.func_name}' имеет некоректные аннотации {invalid_annotations}")

    def __repr__(self):
        return f"FuncDataServer: func_name={self.func_name}, worker_type={self.worker_type}"


class FuncDataClient:
    def __init__(self, serialized_data: dict):
        self.func_name = serialized_data["func_name"]
        self.func_args: list[dict] = serialized_data["func_args"]
        self.func_kwargs: list[dict] = serialized_data["func_kwargs"]

    def _create_func_head(self) -> str:
        args = self.func_args.copy()
        args.extend(self.func_kwargs)
        args_func = ", ".join(self._create_str_param_head(param) for param in args)

        return f"def {self.func_name}({args_func}):"

    def _create_func_body(self) -> str:
        args = ", ".join(arg["name"] for arg in self.func_args)
        kwargs = ", ".join(f"'{arg['name']}': {arg['name']}" for arg in self.func_kwargs)
        return f"\treturn AwaitableTask('{self.func_name}', ({args}), {{{kwargs}}})"

    def create_func(self):
        return self._create_func_head() + "\n" + self._create_func_body()

    def _create_str_param_head(self, param: dict):
        result = param["name"]

        if "default" in param:
            result += f"={param['default']}"
        if "annotation" in param:
            result += f":{param['annotation']}"
        return result
