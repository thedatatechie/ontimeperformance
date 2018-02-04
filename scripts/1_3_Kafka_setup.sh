#!/bin/bash -x

# Start the Kafka cluster
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties

# Create Kafka topics for each dataset
./bin/kafka-topics.sh --list --zookeeper [localhost]:2181
./bin/kafka-topics.sh --create --zookeeper [localhost]:2181 --replication-factor 2 --partitions 3 --topic airports_kafka
./bin/kafka-topics.sh --create --zookeeper [localhost]:2181 --replication-factor 2 --partitions 3 --topic carriers_kafka
./bin/kafka-topics.sh --create --zookeeper [localhost]:2181 --replication-factor 2 --partitions 3 --topic planedate_kafka
./bin/kafka-topics.sh --create --zookeeper [localhost]:2181 --replication-factor 2 --partitions 3 --topic otp_kafka


# From working directory insert the data into Kafka topics. Update the reference paths accordingly
cat airports.csv | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic airports_kafka --new-producer
cat carriers.csv | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic carriers_kafka --new-producer
cat plane-data.csv | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic planedate_kafka --new-producer
cat 2008.csv | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic otp_kafka --new-producer
cat 2007.csv | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic otp_kafka --new-producer




