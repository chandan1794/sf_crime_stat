# SF Crime Statistics with Spark Streaming
## Project Overview
In this project, I was provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, 
and I have provided statistical analyses of the data using Apache Spark Structured Streaming. 
I have created a Kafka server to produce data, and ingest data through Spark Structured Streaming.


## Development Environment
I chose to setup the environment locally rather than on workspace
Spark 2.4.4
Scala 2.13.0
Java 1.8.0_171
Kafka build with Scala 2.11
Python 3.7.3


## Beginning the Project
This project requires creating topics, starting Zookeeper and Kafka server, and Kafka bootstrap server. 
Use the commands below to start Zookeeper and Kafka server.
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Now start the server using this Python command:
```
python producer_server.py
```

### Step 1
The first step is to build a simple Kafka server.
To run, use the command 
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic service-calls --from-beginning
```

### Step 2
Apache Spark already has an integration with Kafka Brokers, hence we will not need a separate Kafka Consumer.
Do a spark-submit using this command
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 --master local[4] data_stream.py
```

### Step 3
To check if `producer_server.py` is working properly, run command below,
```
python consumer_server.py
```

## Screenshots
All the screenshots related to the processes and logs are in the [screenshots](/screenshots). Please refer to them.
