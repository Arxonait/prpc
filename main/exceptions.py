class NotFoundFunc(Exception):
    def __init__(self, func_name: str):
        self.func_name = func_name

    def __str__(self):
        return f"Exception NotFoundFunc: func name '{self.func_name}'"
