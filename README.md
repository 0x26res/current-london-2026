# Current London 2026 Example

Python data streaming example used to illustrate a talk at Current London 2026 conference.

## Architecture Overview

We will connect to Coinbase's websocket API to receive crypto market updates in real time.
In order to share this data with other services and decouple producers from consumers, we'll publish this data over [Kafka](https://kafka.apache.org/), as json.

We'll then run a python application job that will read the data from Kafka and run some analytics workload on it

```mermaid
flowchart TD
    A[Coinbase] -->|Websocket| B(websocket.py)
    B -->|Kafka| C(analytics.py)
```
  
## Initial Set Up

You'll need:

- Git
- Python (at least 3.10)
- Docker to run a Kafka cluster

The code for this tutorial is available on [github](https://github.com/0x26res/current-london-2026)

### Clone the repo

```shell
git https://github.com/0x26res/current-london-2026
```

### Set Up the Virtual Environment

```shell
uv sync
```

### Set Up Kafka

We use the [kafka-kraft](https://github.com/bashj79/kafka-kraft-docker) docker image to run a super simple Kafka cluster.
To start the cluster the first time:

```shell
docker run --name=simple_kafka -p 9092:9092 -d bashj79/kafka-kraft
```

And the second time:

```shell
docker run simple_kafka
```

Once started you can create a Kafka topics called `ticker` and `status`.

```shell
docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=ticker --partitions=1 --replication-factor=1
docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=status --partitions=1 --replication-factor=1
```

### Publish Coinbase's Market Data on Kafka

In this step, we'll run a simple python job that listen to Coinbase's Websocket market data API, and publish the data on the `ticker` Kafka topic.

```shell
uv run python ./websocket_feed.py
```

You should now be able to see the Coinbase data streaming on Kafka.

```shell
docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic=ticker 
docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic=status 
```

### Run the dashboard

```shell
uv run python ./analytics.py
```
