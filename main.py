from kafka import Producer
import threading
from resilient_thread import ResilientThread

from supervisor import ThreadSupervisor


if __name__ == "__main__":
    producer = Producer()

    for i in range(1, 101):
        producer.produce({"value": i})

    supervisor = ThreadSupervisor()
    supervisor.add_task(producer.poll_loop)

    supervisor_thread = supervisor.start_workers()
    supervisor_thread.join()

    # thread = ResilientThread(producer.poll_loop)
    # thread.start()
    # thread.join()
