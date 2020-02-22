import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = "udacity.project.spark-streaming.police"
