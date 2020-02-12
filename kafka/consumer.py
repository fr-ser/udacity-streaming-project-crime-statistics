import json
import sys
import time

from confluent_kafka import Consumer
from loguru import logger

from config import (
    KAFKA_BROKER_URL,
    KAFKA_TOPIC, LOG_LEVEL,
)


def consume():
    """Consumes data from the Kafka Topic"""
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKER_URL,
        "group.id": f"crime-consumer-server-{time.time()}",
    })
    c.subscribe([KAFKA_TOPIC])

    while True:
        messages = c.consume(num_messages=1, timeout=5)
        for message in messages:
            if message.error() is not None:
                logger.error(f"error from consumer {message.error()}")
            else:
                logger.info(f"consumed message {message.key()}: {json.loads(message.value())}")


if __name__ == "__main__":
    # remove the default loguru logger (cannot change its level otherwise and stream)
    logger.remove()
    logger.add(sys.stdout, level=LOG_LEVEL)

    consume()
