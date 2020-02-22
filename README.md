# Crime Statistics Analysis with Apache Spark and Kafka

## Starting the project

### Requirement

The main data source file is not included in the repo due to its size.

In order for the code to work the file with the name `police-department-calls-for-service.json`
needs to be put into the root directory of the repo (same folder as the `docker-compose.yaml`).

Please also look at [Watermarking](#watermarking) regarding the file contents.

### Startup

Use the start command which starts a docker-compose cluster: `./start.sh`

The first start downloads the docker images. Especially the spark image with a jupyter notebook is
fairly large and the download can take a while (progress is shown as output).

## Screenshots

Required screenshots have been saved to the [screenshots folder](./screenshots)

### Spark streaming tab

I attached a screenshot of the SQL tab as structured streaming
(in contrast to the old and almost deprecated Dstream) has no streaming tab.

## Questions

The questions have been answered in the [question markdown file](./questions.md)

## Kafka configuration

As the dockerized kafka verison has been used all configuration has been done with environment
variables and can be found in the [docker-compose file](./docker-compose.yaml).

Therefore no `kafka/config/server.properties` or `kafka/config/zookeeper.properties` are present.

## Watermarking

Watermarking sounds like a good thing to do in streaming applications.

But the problem was with the input data here. The provided JSON file was sorted by timestamp in
descending order.

A memory efficient way of loading the file and sending events one by one would start naturally
from the top of the file. Through this the newest event would be sent first and watermarking would
basically drop all incoming events.

To solve this the producer code loads the while JSON into memory (uncool) and then publishes events
from the resulting list in reversed order.
