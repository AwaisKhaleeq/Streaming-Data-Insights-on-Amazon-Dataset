#!/bin/bash

# Activate virtual environment
source myenv/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

# Change to Kafka directory
cd /home/awais-khaleeq/kafka || { echo "Failed to change to Kafka directory"; exit 1; }

# Start ZooKeeper
echo "Starting ZooKeeper..."
./bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
ZK_PID=$!

# Start Kafka server
echo "Starting Kafka server..."
./bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
KAFKA_PID=$!

# Wait for Kafka to start
echo "Waiting for Kafka server to start..."
sleep 30

# Run Python scripts
echo "Running Python scripts..."
python3 1ProducerAss.py >> producer.log 2>&1 &
python3 1ConsumerAss.py >> consumer1.log 2>&1 &
python3 2ConsumerAss.py >> consumer2.log 2>&1 &
python3 3ConsumerAss.py >> consumer3.log 2>&1 &

# Wait for scripts to finish
wait

# Deactivate virtual environment
deactivate

# Stop Kafka and ZooKeeper
echo "Stopping Kafka and ZooKeeper..."
kill $KAFKA_PID $ZK_PID

