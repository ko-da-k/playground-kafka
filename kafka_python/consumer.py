import json
from pathlib import Path

from confluent_kafka import Consumer

TOPIC = "transaction-topic1"
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
    cfg = read_config(Path(__file__).parent / ".confluent/librdkafka.config")
    cfg["auto.offset.reset"] = "earliest"
    cfg["group.id"] = "example_group"
    consumer = Consumer(cfg)

    consumer.subscribe([TOPIC])
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                print("Waiting for message or event/error in poll()")
            elif msg.error():
                print(f"error: {msg.error()}")
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data["count"]
                total_count += count
                print(
                    f"consumed record with key {record_key} and value {record_value} "
                    f"and update total count to {total_count}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
