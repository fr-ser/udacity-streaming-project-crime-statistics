import time

from confluent_kafka import admin, KafkaException

from config import KAFKA_BROKER_URL, KAFKA_TOPIC


def check_kafka(timeout=30):
    end_time = time.time() + timeout

    client = admin.AdminClient({"bootstrap.servers": KAFKA_BROKER_URL})

    print(f"Waiting for kafka")
    while time.time() < end_time:
        try:

            cluster_meta_data = client.list_topics(timeout=1)
            if KAFKA_TOPIC in cluster_meta_data.topics.keys():
                print("Kafka is up!")
                return
            time.sleep(0.3)
        except KafkaException:
            time.sleep(0.3)
            continue
    else:
        raise Exception(f"Could not fin {KAFKA_TOPIC} in topics")


if __name__ == "__main__":
    check_kafka()
