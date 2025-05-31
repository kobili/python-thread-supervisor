import json
from random import randint
import threading

class Producer:
    """
    A mock Kafka producer
    """

    def __init__(self) -> None:
        self._queue_lock = threading.Lock()
        self._queue: list[str] = []

    def produce(self, message: str | dict):
        if isinstance(message, dict):
            message = json.dumps(message)

        with self._queue_lock:
            self._queue.append(message)

    def poll(self):
        rng = randint(1, 100)
        if rng <= 1:
            raise Exception("Exception happened in `poll`")

        if self._queue:
            with self._queue_lock:
                if self._queue:
                    message = self._queue.pop(0)
                    print(f"Polled and found message: {message}")
                    return
                
        print("No messages found during `poll`")