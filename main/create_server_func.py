import datetime
import os

from main.app_client import get_function_server

current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "server_func.py")

lines = [
        "from main.app_client import AwaitableTask",
        "from typing import Any"
    ]

awaitable = get_function_server()
awaitable.wait_result_task(datetime.timedelta(seconds=30))
result: list[str] = awaitable.get_result()

lines.extend(result)
lines = list(map(lambda item: item + " \n", lines))

with open(file_path, mode="w") as file:
    file.writelines(lines)
