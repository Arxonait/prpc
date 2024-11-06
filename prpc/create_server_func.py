import datetime
import os

from prpc.app_client import get_function_server
from prpc.func_data import FuncDataClient


class ServerFuncPy:
    _FILE_NAME = "server_func.py"
    _based_import_lines = [  # todo remove hardcode
        "from prpc.app_client import AwaitableTask",
        "from typing import *"
    ]
    _TIMEOUT = datetime.timedelta(seconds=30)

    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.file_path = os.path.join(current_dir, self._FILE_NAME)

    def _create_sector_import(self, additional_import_lines: list[str] = None):
        import_lines = []
        import_lines.extend(self._based_import_lines)
        if additional_import_lines and isinstance(additional_import_lines, list):
            import_lines.extend(additional_import_lines)
        return list(map(lambda item: item + " \n", import_lines))

    def _create_sector_func(self, funcs: list[str]):
        separator = "\n\n\n"
        funcs_lines = funcs
        return list(map(lambda item: item + f" {separator}", funcs_lines))

    def _create_file(self, created_func: list[str]):
        separator = "\n\n"
        sector_import = self._create_sector_import()
        sector_funcs = self._create_sector_func(created_func)
        lines = [*sector_import, separator, *sector_funcs, separator]
        with open(self.file_path, mode="w") as file:
            file.writelines(lines)

    def create(self):
        awaitable = get_function_server()
        awaitable.sync_wait_result_task(self._TIMEOUT)

        result: list[dict] = awaitable.get_result()
        created_funcs = [FuncDataClient(result_item).create_func() for result_item in result]
        self._create_file(created_funcs)
        print(f"Created file .py {self.file_path}")


ServerFuncPy().create()



