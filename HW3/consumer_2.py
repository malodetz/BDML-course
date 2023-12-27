import time
import random

from kafka import KafkaConsumer
from functools import wraps


def backoff(tries, sleep):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal tries
            attempt = 1
            while attempt <= tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Retry {attempt}/{tries}, error: {e}, waiting for {sleep} seconds...")
                    time.sleep(sleep)
                    attempt += 1
            raise Exception(f"Failed after {tries} attempts.")
        return wrapper
    return decorator


@backoff(tries=3, sleep=10)
def message_handler(value) -> None:
    error = random.choice([True, False])
    print(value)
    if error:
        raise Exception("Failed to process")


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("itmo2023",
                             group_id='itmo_group1',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        # send to http get (rest api) to get response
        # save to db message (kafka) + external
        message_handler(message)
        print(message)


if __name__ == '__main__':
    create_consumer()
