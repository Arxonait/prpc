import inspect
from typing import NamedTuple


class FuncData(NamedTuple):
    func_name: str
    func_args: dict[str, str]
    return_type: str = "Any"


def convert_func_to_dict(func) -> FuncData:
    name_func = func.__name__
    # Сигнатура функции с аргументами
    func_signature = inspect.signature(func)
    # Список аргументов
    func_args = func_signature.parameters
    # Получаем аннотацию типа возвращаемого значения
    return_annotation = func_signature.return_annotation
    func_data = {
        "func_name": name_func,
        "func_args": func_args
    }

    if return_annotation is not inspect.Signature.empty:
        func_data["return_type"] = str(return_annotation)
    return FuncData(**func_data)


def create_func_head_from_dict(func_data: FuncData) -> str:
    args_func = ", ".join(map(str, func_data.func_args.values()))
    return f"def {func_data.func_name}({args_func}) -> {func_data.return_type}:"

