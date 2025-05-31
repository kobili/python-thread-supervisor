import threading
import time
from typing import Callable, Any

class ResilientThread:
    def __init__(self, target: Callable, args: tuple[Any] | None = None, kwargs: dict[str, Any] | None = None):
        self.target = target

        if args is None:
            args = tuple()
        self.args = args

        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs

        self.thread: threading.Thread | None = None

    def _resilient_task_loop(self):
        while True:
            try:
                self.target(*self.args, **self.kwargs)
            except Exception as e:
                print(f"Task encountered an exception: {e}")
            
            time.sleep(2)

    def start(self):
        self.thread = threading.Thread(target=self._resilient_task_loop)
        self.thread.start()

    def join(self):
        if self.thread is None:
            raise Exception("Thread has not been started yet.")
        
        self.thread.join()
