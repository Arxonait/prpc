import inspect
from typing import NamedTuple


class FuncData:
    def __init__(self, func):
        self.func = func

        self.func_args = {}
        self.name_func: str = None
        self.return_annotation = "Any"
        self.__parse_func_data()

    def __parse_func_data(self):
        self.name_func = self.func.__name__
        # Сигнатура функции с аргументами
        func_signature = inspect.signature(self.func)
        # Список аргументов
        self.func_args = func_signature.parameters

        # Получаем аннотацию типа возвращаемого значения
        return_annotation = func_signature.return_annotation
        if return_annotation is not inspect.Signature.empty:
            self.return_annotation = str(return_annotation)

    def __create_func_head_from_dict(self) -> str:
        args_func = ", ".join(map(str, self.func_args.values()))
        return f"def {self.name_func}({args_func}) -> {self.return_annotation}:"



