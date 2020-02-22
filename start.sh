#!/bin/bash

docker-compose down --volumes
docker-compose build

# start kafka and wait for topics to be created
docker-compose run kafka-cli

# start spark job
docker-compose up -d jupyter
docker-compose exec jupyter /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --master local[*] /opt/spark-apps/data_stream.py
