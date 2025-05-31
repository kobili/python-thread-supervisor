from __future__ import annotations

import threading
import time
from typing import Callable, Iterable, Optional


class ThreadSupervisor:
    def __init__(self):
        self._workers: list[ThreadWorker] = []

        self._worker_lock = threading.Lock()
        self._running = threading.Event()

    def add_task(
        self,
        target: Callable,
        target_args: Optional[Iterable] = None,
        target_kwargs: Optional[dict] = None,
    ):
        """
        Create a `ThreadWorker` instance from the given target
        """

        self._add_worker(
            ThreadWorker(
                target=target,
                target_args=target_args,
                target_kwargs=target_kwargs
            )
        )

    def start_workers(self) -> threading.Thread:
        supervisor_thread = threading.Thread(target=self._start)
        supervisor_thread.start()

        return supervisor_thread

    def stop_workers(self):
        print("Stopping supervisor")
        self._running.clear()

    def _start(self):
        # start each thread worker in `self._workers`
        with self._worker_lock:
            for worker in self._workers:
                worker.start()

        print("Starting supervisor")
        self._running.set()

        self._supervise()

    def _supervise(self):
        while self._running.is_set():
            running_workers: list[ThreadWorker] = []

            with self._worker_lock:
                for worker in self._workers:
                    if not worker.is_active():
                        rerun_worker = self._handle_stopped_worker(worker)
                        if rerun_worker:
                            running_workers.append(rerun_worker)
                    else:
                        running_workers.append(worker)

                # prune stopped workers
                self._workers = running_workers

            time.sleep(2.0)

    def _handle_stopped_worker(self, worker: ThreadWorker) -> Optional[ThreadWorker]:
        """
        Deal with a stopped worker. If the worker had to be restarted, returns the worker
        """

        if worker.is_error():
            return self._retry_worker(worker)

        # do nothing for now
        return None

    def _retry_worker(self, worker: ThreadWorker) -> ThreadWorker:
        """
        Restarts the worker. The worker is then returned
        """

        worker.restart()

        return worker

    def _add_worker(self, worker: ThreadWorker):
        with self._worker_lock:
            self._workers.append(worker)


class ThreadWorker:
    def __init__(
        self,
        target: Callable,
        target_args: Optional[Iterable] = None,
        target_kwargs: Optional[dict] = None,
    ):
        self._target = target

        if target_args is None:
            target_args = ()
        self._target_args = target_args

        if target_kwargs is None:
            target_kwargs = {}
        self._target_kwargs = target_kwargs

        self._thread = self.create_thread()

        self._exception: Optional[Exception] = None

    def _wrap_target(self):
        def wrapper():
            try:
                self._target(*self._target_args, **self._target_kwargs)
            except Exception as e:
                self._exception = e
                raise e

        return wrapper
    
    def create_thread(self) -> threading.Thread:
        return threading.Thread(
            target=self._wrap_target(),
        )

    def start(self):
        """
        Start the thread stored in `self._thread`
        """
        self._thread.start()

    def is_active(self) -> bool:
        return self._thread.is_alive()
    
    def is_error(self) -> bool:
        return not self.is_active() and self._exception is not None
    
    def restart(self):
        """
        Create a new thread and start it.

        Assigns the new thread to `self._thread`
        """

        self._thread = self.create_thread()
        self._exception = None
        self._thread.start()
