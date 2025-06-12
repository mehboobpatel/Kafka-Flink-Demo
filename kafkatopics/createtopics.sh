#!/bin/bash
docker exec -it kafka \
  kafka-topics --create --topic input_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it kafka \
  kafka-topics --create --topic output_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
