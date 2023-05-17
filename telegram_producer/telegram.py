import os
import sys
import time

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.telegram_utils import process_message
from utils.common_utils import create_producer, create_consumer, get_offset_id

from dotenv import load_dotenv

load_dotenv()

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

    producer = create_producer(kafka_bootstrap_servers_=kafka_bootstrap_servers_)
    consumer = create_consumer(kafka_topic_=kafka_topic_,
                               kafka_bootstrap_servers_=kafka_bootstrap_servers_, group_id='telegram_group')

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
    kafka_topic = os.getenv('TELEGRAM_INPUT_TOPIC')
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    client.loop.run_until_complete(main(kafka_topic, kafka_bootstrap_servers))
