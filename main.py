from kafka import Producer
from resilient_thread import ResilientThread


if __name__ == "__main__":
    producer = Producer()

    for i in range(1, 101):
        producer.produce({"value": i})

    thread = ResilientThread(producer.poll_loop)
    thread.start()
    thread.join()
