import datetime
import os

from main.app_client import get_function_server

current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "server_func.py")

head_lines = [
        "from main.app_client import AwaitableTask",
    ]
head_lines = list(map(lambda item: item + " \n", head_lines))
lines = [*head_lines, "\n\n"]

awaitable = get_function_server()
awaitable.sync_wait_result_task(datetime.timedelta(seconds=30))
result: list[str] = awaitable.get_result()

body_lines = result
body_lines = list(map(lambda item: item + " \n\n", body_lines))
lines.extend(body_lines)

with open(file_path, mode="w") as file:
    file.writelines(lines)
