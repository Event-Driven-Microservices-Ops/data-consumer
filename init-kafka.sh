#!/bin/sh

/etc/confluent/docker/run &

echo "Waiting for Kafka broker to fully start..."
cub kafka-ready -b kafka1:9092 1 60

echo "Creating topic 'data'..."
kafka-topics --create --if-not-exists --topic data \
  --bootstrap-server kafka1:9092 \
  --partitions 1 \
  --replication-factor 1

wait