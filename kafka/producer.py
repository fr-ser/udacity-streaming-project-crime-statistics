import json
import sys
import time

import ijson
from confluent_kafka import Producer
from loguru import logger

from config import KAFKA_BROKER_URL, KAFKA_TOPIC, LOG_LEVEL

POLICE_DEPARTMENT_FILE_PATH = "./police-department-calls-for-service.json"


def load_source_data():
    # The ijson implementation works and is very memory efficient
    # but since the data in the JSON file is ordered latest to first this prevents watermarking.
    # Therefore the only option is to load the whole JSON file and publish latest to first

    # with open(POLICE_DEPARTMENT_FILE_PATH) as the_file:
    #     # we are using a partial json parser to not load the whole file into memory
    #     for item in ijson.items(the_file, 'item'):
    #         yield item

    with open(POLICE_DEPARTMENT_FILE_PATH) as the_file:
        event_list = json.load(the_file)
    for item in reversed(event_list):
        yield item


def run():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER_URL})
    for event in load_source_data():
        producer.produce(KAFKA_TOPIC, json.dumps(event))
        logger.debug(f"Producer event: {event}")
        producer.flush()
        # uncomment to simulate live incomming data
        # time.sleep(0.33)


if __name__ == "__main__":
    # remove the default loguru logger (cannot change its level and output otherwise)
    logger.remove()
    logger.add(sys.stdout, level=LOG_LEVEL)

    run()
