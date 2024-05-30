# Streaming-Data-Insights-on-Amazon-Dataset
This project performs frequent itemset analysis on the Amazon Metadata dataset using streaming data techniques. It includes data preprocessing, real-time streaming setup, implementation of Apriori and PCY algorithms, and innovative consumer processing. Results are stored in a NoSQL database, with a bash script to automate the setup.
# Amazon-MetaData-Processing-Using-Kafka
Applying PCY, Apriori and a Dynamic Product Reccomendation Engine

# Building-a-Amazon-MetaData-Dataset-Processor-using-Kafka
The "Building-a-Amazon-MataData-Dataset-Processor-using-Kafka" project focuses on building a complex data pipeline using Kafka. The pipeline will involve setting up a Kafka cluster, creating multiple topics, writing producers to generate data, writing consumers to process the data.

## Operating System: Windows

## Platforms:
- Jupyter Notebook
- Terminal
## Dependencies:
- Kafka
- MongoDB
- Json

## JSON
The data is taken from the original dataset of 105GNB into a smaller chunk of 15GB This conversion allows faster and more easy data processing for student devices.


# Kafka Setup
Ensure that Kafka is installed on your system.

Set up a Kafka cluster with three brokers by following these steps:

Add paths for three new log files.
Assign three different ports for the topics.
Create three new server files with the updated changes.
Start ZooKeeper.

Start the Kafka server.

Create three topics on the partitions of the brokers.

Run the producer and consumer using the provided topics (topic1, topic2, and topic3) using the relevant commands.

Note: The producer fetches data from the specified topics, and the consumer performs operations on the data stored in the cluster.

# MongoDB
Install MongoDB by following the tutorial provided.

Install MongoDB from "https://www.mongodb.com/try/download/community"

#Install the MongoDB CLI
To install the MongoDB CLI, choose one of the following methods:

Install with a package manager like Homebrew, Yum, or Apt.

Download and extract the binary.

Clone the GitHub repository and install the MongoDB CLI with Go.
