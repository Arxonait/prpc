import inspect
from typing import NamedTuple


class FuncData(NamedTuple):
    func_name: str
    func_args: dict[str, str]
    return_type: str = "Any"


def convert_func_to_str(func) -> FuncData:
    name_func = func.__name__
    # Сигнатура функции с аргументами
    func_signature = inspect.signature(func)
    # Список аргументов
    func_args = func_signature.parameters
    # Получаем аннотацию типа возвращаемого значения
    return_annotation = str(func_signature.return_annotation)
    func_data = {
        "func_name": name_func,
        "func_args": func_args
    }

    if return_annotation is not inspect.Signature.empty:
        func_data["return_type"] = return_annotation
    return FuncData(**func_data)


def dev_test_func(zz: int = 5, b="str") -> list[str]:
    print(5)


if __name__ == "__main__":
    result = convert_func_to_str(dev_test_func)
    print(result)
