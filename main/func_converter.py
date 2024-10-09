import inspect
from typing import NamedTuple


class FuncData:
    def __init__(self, func):
        self.func = func

        self.func_args = dict[str, inspect.Parameter]
        self.func_kwargs = dict[str, inspect.Parameter]
        self.func_name: str = None
        self.return_annotation = "Any"
        self.__parse_func_data()

    def __parse_func_data(self):
        self.func_name = self.func.__name__
        # Сигнатура функции с аргументами
        func_signature = inspect.signature(self.func)
        # Список аргументов
        func_args = func_signature.parameters
        self.func_args = dict(filter(lambda item: item[1].default == inspect.Parameter.empty, func_args.items()))
        self.func_kwargs = dict(filter(lambda item: item[1].default != inspect.Parameter.empty, func_args.items()))

        # Получаем аннотацию типа возвращаемого значения
        return_annotation = func_signature.return_annotation
        if return_annotation is not inspect.Signature.empty:
            self.return_annotation = str(return_annotation)

    def __create_func_head(self) -> str:
        args = self.func_args.copy()
        args.update(self.func_kwargs)
        args_func = ", ".join(self.__create_str_param_head(param) for param in args.values())

        return f"def {self.func_name}({args_func}) -> {self.return_annotation}:"

    def __create_func_body(self) -> str:
        args = ", ".join(self.func_args.keys())
        kwargs = ", ".join(f"'{key}': {key}" for key in self.func_kwargs.keys())
        return f"\treturn AwaitableTask('{self.func_name}', ({args}), {{{kwargs}}})"

    def create_func(self):
        return self.__create_func_head() + "\n" + self.__create_func_body()

    def __create_str_param_head(self, param: inspect.Parameter):
        result = param.name

        if param.annotation != inspect.Parameter.empty:
            result += f":{param.annotation.__name__}"
        if param.default != inspect.Parameter.empty:
            result += f"={param.default}"
        return result





if __name__ == "__main__":
    def foo(a, b, c: int, d: str = 5):
        return None

    func_data = FuncData(foo)
    print(func_data.create_func())



