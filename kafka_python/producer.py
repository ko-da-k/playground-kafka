import json
import time
from pathlib import Path

from confluent_kafka import Producer

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
    producer = Producer(cfg)
    for i in range(100):
        for n in range(10):
            record_key = "alice"
            record_value = json.dumps({"count": n})
            print(f"Producing record: {record_key}\t{record_value}")
            producer.produce(
                TOPIC, key=record_key, value=record_value, on_delivery=acked
            )
            producer.poll(0)
        producer.flush()
        print(f"{DELIVERED_RECORD} messages were produced to topic {TOPIC}!")
        time.sleep(1)
