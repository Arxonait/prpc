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
from main.app_server import AppServer
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
...

