kafka-create:
    docker create --name=simple_kafka -p 9092:9092 bashj79/kafka-kraft

kafka-start:
    docker start simple_kafka

kafka-create-topics:
    docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=price --partitions=1 --replication-factor=1
    docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=status --partitions=1 --replication-factor=1


kafka-stream-price:
    docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic=price

kafka-stream-status:
    docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic=status


run-websocket-feed:
    uv run python ./websocket_feed.py

run-analytics-simple:
    uv run python ./analytics.py --dag=simple

run-analytics-complex:
    uv run python ./analytics.py --dag=complex