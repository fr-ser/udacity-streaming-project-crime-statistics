import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

POLICE_DEPARTMENT_FILE_PATH = "./police-department-calls-for-service.json"

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = "udacity.project.spark-streaming.police"
