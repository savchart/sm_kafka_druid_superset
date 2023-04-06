import os
import sys
import json
import time

from telethon.sync import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetHistoryRequest

from kafka import KafkaProducer, KafkaConsumer

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_dir)

from utils.telegram_utils import DateTimeEncoder, process_message, get_offset_id

api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
session_string = os.getenv('SESSION_STRING')
user_input_channel = os.getenv('CHAT_NAME')


async def main(kafka_topic_, kafka_bootstrap_servers_):
    await client.start()
    me = await client.get_me()
    print(f'Client created for {me.username} and {user_input_channel} (channel)')

    my_channel = await client.get_entity(user_input_channel)
    limit = 100

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers_,
        value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8')
    )

    consumer = KafkaConsumer(
        kafka_topic_,
        group_id='my-group',
        auto_offset_reset='latest',
        enable_auto_commit=False,
        bootstrap_servers=kafka_bootstrap_servers_,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

    min_id = get_offset_id(consumer)
    consumer.close()

    while True:
        offset_id = min_id + limit + 1
        history = await client(GetHistoryRequest(
            peer=my_channel, max_id=0, limit=limit,
            offset_id=offset_id, offset_date=None, add_offset=0,
            min_id=min_id, hash=0
        ))

        if not history.messages:
            time.sleep(600)
            continue
        messages = history.messages[::-1]
        for message in messages:
            message_kafka_dict = await process_message(message.to_dict(), client)
            try:
                producer.send(kafka_topic_, message_kafka_dict)
                print(f"Message {message_kafka_dict['id']} sent to Kafka")
            except Exception as e:
                print(f"Error sending message {message_kafka_dict['id']} to Kafka: {e}")
            producer.flush()
        min_id += limit

    producer.close()


with TelegramClient(StringSession(session_string), api_id, api_hash) as client:
    kafka_topic = os.getenv('KAFKA_INPUT_TOPIC')
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    client.loop.run_until_complete(main(kafka_topic, kafka_bootstrap_servers))
