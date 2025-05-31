import atexit

from kafka import Producer
from supervisor import ThreadSupervisor


if __name__ == "__main__":
    producer = Producer()

    for i in range(1, 101):
        producer.produce({"value": i})

    supervisor = ThreadSupervisor()

    try:
        supervisor.add_task(producer.poll_loop)
        atexit.register(supervisor.stop_workers)

        supervisor_thread = supervisor.start_workers()
        supervisor_thread.join()
    except KeyboardInterrupt:
        supervisor.stop_workers()
