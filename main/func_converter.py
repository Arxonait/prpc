import inspect


class FuncData:
    def __init__(self, func, worker_type: str):
        self.func = func
        self.worker_type = worker_type

        self.func_args = dict[str, inspect.Parameter]
        self.func_kwargs = dict[str, inspect.Parameter]
        self.func_name: str = None
        self.__parse_func_data()

    def __parse_func_data(self):
        # todo parse dock string
        self.func_name = self.func.__name__

        # Сигнатура функции с аргументами
        func_signature = inspect.signature(self.func)

        # Список аргументов
        func_args = func_signature.parameters
        self.func_args = dict(filter(lambda item: item[1].default == inspect.Parameter.empty, func_args.items()))
        self.func_kwargs = dict(filter(lambda item: item[1].default != inspect.Parameter.empty, func_args.items()))

    def __create_func_head(self) -> str:
        args = self.func_args.copy()
        args.update(self.func_kwargs)
        args_func = ", ".join(self.__create_str_param_head(param) for param in args.values())

        return f"def {self.func_name}({args_func}):"

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
