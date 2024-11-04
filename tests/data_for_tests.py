import time

CONFIG_BROKER_REDIS = "redis://localhost:6379/0"
TEST_NAME_QUEUE = "test_test"
GROUP_NAME = "TEST_GROUP"

FRAMEWORK_NAME_QUEUE = f"prpc_{TEST_NAME_QUEUE}"
FRAMEWORK_NAME_QUEUE_FEEDBACK = f"prpc_feedback_{TEST_NAME_QUEUE}"
FRAMEWORK_NAME_QUEUE_RAW = f"prpc_raw_{TEST_NAME_QUEUE}"
FRAMEWORK_NAME_QUEUE_FEEDBACK_RAW = f"prpc_feedback_raw_{TEST_NAME_QUEUE}"


def hello_world():
    return "hello_world"


def summ(a, b):
    return a + b


def greeting(name, part_of_day):
    return f"Hi {name}, have a nice {part_of_day}"


def func_time(time_sec):
    time.sleep(time_sec)
    return True

