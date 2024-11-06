# О PRPC
PRPC (python RPC) - это реализация технологии RPC исключительно для python.

Сравнение с gRPC
- Используются брокеры сообщений для передачи данных, а не HTTP 2.0
- Не нужно писать protobuf script для совместной работы клиента и сервера, достаточно знать имя очередей

# Очереди
Основноная очередь - `prpc_{queue_name}`

Для получения результатов от сервера используется дополнительная очередь - `prpc_feedback_{queue_name}`

Обратная очередь и основноная очередь - хранят в себе сообщения определенного формата (PRPCMessage) сереализованного через jsonpickle

# Workers
Каждая функция исполнятся определенным типом worker

Типы workers:
1. thread (concurrent.features) - для IO 
2. process (concurrent.features) - для IO/CPU 
3. async (asyncio) - для IO

# Brokers
Поддерживаемые brokers
1. Redis (streams | key-value)
    streams используется для основной очереди

    key-value используется для хранения результатов (сделано специально, чтобы упростить работу для клиента) - `prcp_message_feedback_{message_id}`
2. Kafka

# Установка зависимостей

```
pip install -r requirements.txt
```

# Сервер
Сервер реализован как асинхронный framework

```python
from prpc.app_server import AppServer
import time
import datetime

app = AppServer("redis", "redis://localhost:6379/0", "thread", 4, datetime.timedelta(seconds=100))


@app.decorator_reg_func("process")
def summ(a: int | float, b, z=0.5):
    return a + b + z


@app.decorator_reg_func()
def hello_world():
    time.sleep(0.5)
    return "hello_world"


if __name__ == "__main__":  # Обязательно использовать точку входа (особенно если используется worker типа `process`)
    app.start()
```

Все зарегистрированные функции должны принимать и возвращать данные с примитивными типами (int, float, str, bool, None, byte, list, dict и тд)

Типы данных библиотек (например datetime, uuid) будет восприниматься как dict

## Основные параметры AppServer
1. type_broker: str - 'redis' or 'kafka'
2. broker_url: str - 'redis://localhost:6379/0' or 'localhost:9092'
3. default_type_worker: str - Типы workers: thread, process, async. будет примениться ко всем зарегистрированным функциям, которые не имеют определенного worker
4. max_number_worker: int - кол-во workers которые могут одновременно работать и принимать новые сообщение.
5. timeout_worker: datetime | None - максимальное время работы worker над одним сообщением

## ENV server
Для точной настройки сервера можно установить env:
1. PRPC_INSTANCE_NAME - str - имя для экземпляра сервера. Используется redis. Если не установленно, то будет сгенерировано при каждом запуске сервера новое имя - `str(uuid.uuid4())`
2. PRPC_REDIS_EXPIRE_MESSAGE_FEEDBACK_SEC - int - кол-во секунд для хранение результата в redis. По умолчанию `datetime.timedelta(hours=6).total_seconds()`
3. PRPC_REDIS_HEARTBEAT_INTERVAL_SEC - int - кол-во секунд для подтверждения что сообщение еще обрабатывается worker. По умолчанию 15
4. PRPC_REDIS_RECOVER_INTERVAL_SEC - int - кол-во секунд для перехвата контроля над сообщением (восстановление незавершенных сообщений). По умолчанию 45. Должно быть всегда больше чем `PRPC_REDIS_HEARTBEAT_INTERVAL_SEC`
5. PRPC_KAFKA_FEEDBACK_TOPIC_NUMBER_PARTITIONS - int - кол-во partitions в топиках-ответов. По умолчанию 4
6. PRPC_KAFKA_QUEUE_TOPIC_NUMBER_PARTITIONS - int - кол-во partitions в основных топиках. По умолчанию None - если оно не установленно, то кол-во partitions будет определено по максимальному кол-во worker
7. PRPC_KAFKA_REPLICATION_FACTOR - int - replication factor for kafka

# Клиент
Чтобы обеспечить работу клиента нужно: 
1. Установить env

    Для Redis
    ```
    PRPC_TYPE_BROKER=redis
    PRPC_URL_BROKER=redis://localhost:6379/0
    PRPC_QUEUE_NAME=task_prpc
    ```
    Для Kafka
    ```
    PRPC_TYPE_BROKER=kafka
    PRPC_URL_BROKER=localhost:9092
    PRPC_QUEUE_NAME=task_prpc
    ```

2. Запустить код для создания файла `server_func.py`, из которого можно импортировать функции сервера
    ```
    python -m prpc.create_server_func
    ```
## Пример клиента
```python

```

# Использование prpc c другими яп
Реализована возможность отправлять запросы к серверу с других языков программирования и получать результат.

Для этого необходимо отправить сообщение в очередь `prpc_raw_{queue_name}` и получить результат из `prpc_feedback_raw_{queue_name}`

Пример: 
1. Сформировать сообщение (структуру данных)

    ```python
    import uuid
    
    message = {
        "func_name": "hello_world",
        "func_args": [],
        "func_kwargs": {},
        "message_id": str(uuid.uuid4())
    }
    ```
2. Отправить сообщение в очередь
    
    Сообщение должно быть в формате json
    
    В случае kafka: `producer.send(f"prpc_raw_{queue_name}", json.dumps(message))`
    
    В случае redis: `redis.xadd(f"prpc_raw_{queue_name}", {"message": json.dumps(message)})`
    ```python
    import json
    import redis
    
    queue_name = "task_prpc"
    
    client = redis.from_url("redis://localhost:6379/0")
    client.xadd(f"prpc_raw_{queue_name}", {"message": json.dumps(message)})
    ```
3. Получение результатов

    Результат будет в формате json

    Пример результата
    ```json
    {
      "func_name": "hello_world",
      "func_args": [],
      "func_kwargs": {},
      "message_id": "uuid4",
      "result": "hellp_world",
      "exception_info": null,
      "date_create_message": "2024-11-06T12:00:00.0+03:00",
      "date_done_message": "2024-11-06T12:00:05.0+03:00"
    }
    ```
    Возможные значения:
    - func_name - string
      - func_args - list
      - func_kwargs - dict
      - message_id - string format uuid
      - result - Any or null
      - exception_info - string or null
      - date_create_message - string format isoformat
      - date_done_message - string format isoformat

## kafka
Лучше всего использовать offset по timestamp (date send message) - оптимизация
```python
import json

queue_name = "prpc_task"
consumer = KafkaConsumer(
            f"prpc_feedback_raw_{queue_name}",
            bootstrap_servers="localhost:9092",
            auto_offset_reset='earliest',
        )

message_id = "uuid4"

for message in consumer:
    message = json.loads(message)
    if message_id == message["message_id"]:
        break
```

## redis
```python
import json

queue_name = "prpc_task"
message_id = "uuid4"

client = redis.from_url("redis://localhost:6379/0")
message = redis.get(f"prpc_feedback_raw_{queue_name}_{message_id}")
if message is not None:
    message = json.loads(message)
```