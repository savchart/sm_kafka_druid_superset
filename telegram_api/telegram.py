import json

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
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
from utils.telegram_utils import get_api_id, get_api_hash, get_phone, get_username, get_chat_name

api_id = get_api_id()
api_hash = get_api_hash()
phone = get_phone()
username = get_username()
client = TelegramClient(username, api_id, api_hash)


async def main(phone_, kafka_topic_, kafka_bootstrap_servers_):
    print(f'Client created for {username} (phone: {phone_})')
    await client.start()
    if not await client.is_user_authorized():
        await client.send_code_request(phone_)
        try:
            await client.sign_in(phone_, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    user_input_channel = get_chat_name()

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


with client:
    kafka_topic = 'telegram-messages'
    kafka_bootstrap_servers = ['kafka:9092']
    client.loop.run_until_complete(main(phone, kafka_topic, kafka_bootstrap_servers))
