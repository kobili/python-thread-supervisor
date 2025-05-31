from kafka import Producer


if __name__ == "__main__":
    producer = Producer()

    for i in range(1, 101):
        producer.produce({"value": i})

    while True:
        producer.poll()
