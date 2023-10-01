import json
import time
from pathlib import Path

from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError

INPUT_TOPIC = "transaction-topic1"
OUTPUT_TOPIC = "transaction-topic2"
DELIVERED_RECORD = 0


def read_config(path: Path) -> dict:
    cfg = {}
    with path.open() as f:
        for l in f:
            if len(l) != 0 and l[0] != "#":
                param, val = l.strip().split("=", 1)
                cfg[param] = val.strip()
    return cfg


def acked(err, msg):
    global DELIVERED_RECORD
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        DELIVERED_RECORD += 1
        print(
            f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}"
        )


if __name__ == "__main__":
    cfg_producer = read_config(Path(__file__).parent / ".confluent/librdkafka.config")
    cfg_producer['transactional.id'] = 'producer_transaction.py'
    producer = Producer(cfg_producer)

    cfg_consumer = read_config(Path(__file__).parent / ".confluent/librdkafka.config")
    cfg_consumer["auto.offset.reset"] = "latest"
    cfg_consumer["group.id"] = "example_group"
    cfg_consumer["enable.auto.commit"] = False
    cfg_consumer["enable.partition.eof"] = True
    consumer = Consumer(cfg_consumer)

    producer.init_transactions()
    consumer.assign([TopicPartition(INPUT_TOPIC, 2)])  # alice in partition 2
    total_count = 0
    eof = {}
    try:
        for i in range(100):
            producer.poll(0)

            # consumer process
            msg = consumer.poll(1)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            topic = msg.topic()
            partition = msg.partition()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof[(topic, partition)] = True
                    print(f"Reached the end of {topic} [{partition}] at {msg.offset()}")
                    if len(eof) == len(consumer.assignment()):
                        print("Reached end of input")
            eof.pop((topic, partition), None)  # clear partition

            producer.begin_transaction()
            # produce message
            producer.poll(0)
            record_key = "alice"
            record_value = json.dumps({"count": i})
            print(f"Producing record: {record_key}\t{record_value}")
            producer.produce(
                OUTPUT_TOPIC, key=record_key, value=record_value, on_delivery=acked
            )
            total_count += 1
            print(f"commit transaction with {total_count} at input offset {msg.offset()}")

            # Sends a list of topic partition offsets to the consumer group
            # coordinator for group_metadata and marks the offsets as part
            # of the current transaction.
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),  # data like TopicPartition(topic=INPUT_TOPIC,partition=2,offset=816)
                consumer.consumer_group_metadata()
            )
            producer.commit_transaction()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
