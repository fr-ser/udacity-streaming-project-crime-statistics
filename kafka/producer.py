import json
import sys
import time

import ijson
from confluent_kafka import Producer
from loguru import logger

from config import (
    KAFKA_BROKER_URL,
    POLICE_DEPARTMENT_FILE_PATH,
    KAFKA_TOPIC, LOG_LEVEL,
)


def load_source_data():
    with open(POLICE_DEPARTMENT_FILE_PATH) as the_file:
        # we are using a partial json parser to not load the whole file into memory
        for item in ijson.items(the_file, 'item'):
            yield item


def run():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER_URL})
    for event in load_source_data():
        producer.produce(KAFKA_TOPIC, json.dumps(event))
        logger.debug(f"Producer event: {event}")
        time.sleep(0.33)


if __name__ == "__main__":
    # remove the default loguru logger (cannot change its level and output otherwise)
    logger.remove()
    logger.add(sys.stdout, level=LOG_LEVEL)

    run()
