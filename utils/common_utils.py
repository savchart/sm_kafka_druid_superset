import sys
import time
import json
from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


def create_producer(retry_attempts=5, retry_delay=10, kafka_bootstrap_servers_=None):
    producer = None
    for _ in range(retry_attempts):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers_,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8')
            )
            print(f'Kafka producer created for bootstrap servers: {kafka_bootstrap_servers_}')
            break
        except KafkaError as e:
            print(f"Error creating Kafka producer: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    if producer is None:
        print(f"Failed to connect to Kafka broker after {retry_attempts} attempts.")
        sys.exit(1)

    return producer


def create_consumer(offset='earliest', retry_attempts=5, retry_delay=10, kafka_topic_=None,
                    kafka_bootstrap_servers_=None, group_id=None, max_poll_interval_ms=600000):
    consumer = None
    for _ in range(retry_attempts):
        try:
            consumer = KafkaConsumer(
                kafka_topic_,
                group_id=group_id,
                auto_offset_reset=offset,
                enable_auto_commit=True,
                auto_commit_interval_ms=10000,
                bootstrap_servers=kafka_bootstrap_servers_,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_interval_ms=max_poll_interval_ms
            )
            print(f'Kafka consumer created for {kafka_topic_} topic, {kafka_bootstrap_servers_} bootstrap servers')
            break
        except KafkaError as e:
            print(f"Error creating Kafka consumer: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    if consumer is None:
        raise KafkaError("Failed to create Kafka consumer after all retry attempts")

    return consumer


def get_offset_id(consumer):
    last_successful_offset = 0
    partition = None
    for _ in range(5):
        try:
            consumer.poll(timeout_ms=100, max_records=1)
            partition = consumer.assignment()
            break
        except KafkaTimeoutError:
            time.sleep(10)
    partition = list(partition)[0]
    end_offset = consumer.end_offsets([partition])
    last_id = list(end_offset.values())[0]
    if last_id == 0:
        return last_successful_offset
    consumer.seek(partition, last_id - 1)
    last_successful_offset = next(consumer).value['id']

    return last_successful_offset


def flatten_json(nested_json, key_prefix=''):
    flattened_dict = {}
    for key, value in nested_json.items():
        new_key = key_prefix + key
        if isinstance(value, dict):
            flattened_dict.update(flatten_json(value, new_key + '_'))
        else:
            flattened_dict[new_key] = value
    return flattened_dict
