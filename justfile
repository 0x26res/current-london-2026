kafka-create:
    docker create --name=simple_kafka -p 9092:9092 bashj79/kafka-kraft
kafka-start:
    docker start simple_kafka
kafka-logs:
    docker logs -f simple_kafka

kafka-create-topics: kafka-create-topic-price kafka-create-topic-status
kafka-create-topic-price: (kafka-create-topic "price")
kafka-create-topic-status: (kafka-create-topic "status")
kafka-create-topic topic:
    docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic={{topic}} --partitions=1 --replication-factor=1

kafka-stream-price: (kafka-stream "price")
kafka-stream-status: (kafka-stream "status")
kafka-stream topic:
    docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server=localhost:9092 --topic={{topic}} | jq

run-websocket-feed:
    uv run python ./websocket_feed.py

run-analytics-simple: (run-analytics "simple")
run-analytics-complex: (run-analytics "complex")

run-analytics dag:
    uv run python ./analytics.py --dag={{dag}}

test-benchmark:
    uv run pytest tests/test_benchmark.py

test-unit:
    PYTHONPATH=. uv run pytest tests --ignore-glob="*test_benchmark*"
