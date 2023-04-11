import os
import sys
import json
import time

from telethon.sync import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetHistoryRequest

from kafka import KafkaProducer, KafkaConsumer, errors

from telegram_utils import DateTimeEncoder, process_message, get_offset_id

from dotenv import load_dotenv

load_dotenv()
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_dir)

api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
session_string = os.getenv('SESSION_STRING')
user_input_channel = os.getenv('CHAT_NAME')


async def main(kafka_topic_, kafka_bootstrap_servers_):
    await client.start()
    me = await client.get_me()
    print(f'Client created for {me.username} and {user_input_channel} (channel)')

    my_channel = await client.get_entity(user_input_channel)
    limit = 1000

    producer = None
    consumer = None
    retry_attempts = 5
    retry_delay = 10

    for _ in range(retry_attempts):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers_,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8')
            )
            break
        except errors.NoBrokersAvailable:
            print("No brokers available. Retrying in {} seconds...".format(retry_delay))
            time.sleep(retry_delay)
    else:
        print("Failed to connect to Kafka broker after {} attempts.".format(retry_attempts))
        sys.exit(1)
    print(f'Kafka producer created for {kafka_topic_} topic, {kafka_bootstrap_servers_} bootstrap servers')

    for _ in range(retry_attempts):
        try:
            consumer = KafkaConsumer(
                kafka_topic_,
                group_id='my-group',
                auto_offset_reset='latest',
                enable_auto_commit=False,
                bootstrap_servers=kafka_bootstrap_servers_,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            )
            break
        except errors.NoBrokersAvailable:
            print("No brokers available. Retrying in {} seconds...".format(retry_delay))
            time.sleep(retry_delay)
    print(f'Kafka consumer created for {kafka_topic_} topic, {kafka_bootstrap_servers_} bootstrap servers')

    last_processed_id = get_offset_id(consumer)
    consumer.close()
    print(f'Last processed message id: {last_processed_id} (from Kafka)')

    while True:
        history = await client.get_messages(my_channel, limit=limit, min_id=last_processed_id, reverse=True)
        if not history:
            print('No messages, sleeping for 60 seconds...')
            time.sleep(60)
            continue

        for message in history:
            message_kafka_dict = await process_message(message.to_dict(), client)
            try:
                producer.send(kafka_topic_, message_kafka_dict)
                print(f"Message {message_kafka_dict['id']} sent to Kafka")
            except Exception as e:
                print(f"Error sending message {message_kafka_dict['id']} to Kafka: {e}")
            producer.flush()
        last_processed_id = history[-1].id

    producer.close()


with TelegramClient(StringSession(session_string), api_id, api_hash, connection_retries=5) as client:
    kafka_topic = os.getenv('KAFKA_INPUT_TOPIC')
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    client.loop.run_until_complete(main(kafka_topic, kafka_bootstrap_servers))
