import datetime
import os

from main.app_client import get_function_server
from main.func_module import FuncDataClient

current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "server_func.py")

head_lines = [
        "from main.app_client import AwaitableTask",
    ]
head_lines = list(map(lambda item: item + " \n", head_lines))
lines = [*head_lines, "\n\n"]

awaitable = get_function_server()
awaitable.sync_wait_result_task(datetime.timedelta(seconds=30))
result: list[dict] = awaitable.get_result()
result = [FuncDataClient(result_item).create_func() for result_item in result]

body_lines = result
body_lines = list(map(lambda item: item + " \n\n", body_lines))
lines.extend(body_lines)

with open(file_path, mode="w") as file:
    file.writelines(lines)
print(f"Created file .py {file_path}")
