#!/bin/bash

python wait_for_kafka.py

exec "$@"
