import json

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)

from kafka import KafkaProducer

import os
import sys

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_dir)

from utils.telegram_utils import DateTimeEncoder

api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
session_string = os.getenv('SESSION_STRING')


async def main(kafka_topic_, kafka_bootstrap_servers_):
    await client.start()
    user_input_channel = os.getenv('CHAT_NAME')
    me = await client.get_me()
    print(f'Client created for {me.username} and {user_input_channel} (channel)')

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    offset_id = 0
    limit = 100
    total_messages = 0
    total_count_limit = 0

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers_,
        value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8')
    )

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages, end='\r')
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id, offset_date=None, add_offset=0,
            limit=limit, max_id=0, min_id=0, hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            producer.send(kafka_topic_, message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages += len(messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    producer.flush()
    producer.close()


with TelegramClient(StringSession(session_string), api_id, api_hash) as client:
    kafka_topic = os.getenv('KAFKA_TOPIC')
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    client.loop.run_until_complete(main(kafka_topic, kafka_bootstrap_servers))
