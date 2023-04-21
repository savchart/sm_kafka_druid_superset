import os
import time
import sys
import requests

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.common_utils import create_producer, create_consumer, get_offset_id
from utils.discord_utils import process_message

from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID'))
KAFKA_DISCORD_TOPIC = os.getenv('KAFKA_DISCORD_INPUT_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')


def main(kafka_topic_, kafka_bootstrap_servers_):
    headers = {
        "Authorization": f"{TOKEN}",
        "Content-Type": "application/json"
    }

    def get_messages(channel_id, last_message_id=None):
        messages_url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        params = {"limit": 100}
        if last_message_id:
            params["after"] = last_message_id
        response = requests.get(messages_url, headers=headers, params=params)
        return response.json()

    producer = create_producer(kafka_topic_=kafka_topic_, kafka_bootstrap_servers_=kafka_bootstrap_servers_)
    consumer = create_consumer(kafka_topic_=kafka_topic_, kafka_bootstrap_servers_=kafka_bootstrap_servers_)

    last_processed_id = get_offset_id(consumer)
    consumer.close()

    print(f'Last processed message id: {last_processed_id} (from Kafka)')

    last_message_id = last_processed_id

    while True:
        messages = get_messages(CHANNEL_ID, last_message_id)
        if not messages:
            print('No messages, sleeping for 60 seconds...')
            time.sleep(60)
            continue

        for message in messages:
            message_kafka_dict = process_message(message)
            try:
                producer.send(kafka_topic_, message_kafka_dict)
                print(f"Message {message_kafka_dict['id']} sent to Kafka")
            except Exception as e:
                print(f"Error sending message {message_kafka_dict['id']} to Kafka: {e}")
            producer.flush()
            last_message_id = message["id"]

    producer.close()


main(KAFKA_DISCORD_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
