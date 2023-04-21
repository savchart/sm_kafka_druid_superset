import json
from datetime import datetime



class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


def get_offset_id(consumer):
    last_successful_offset = 0
    consumer.poll(timeout_ms=100, max_records=1)
    partition = list(consumer.assignment())[0]
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


async def process_message(message_dict, client):
    message_flat = flatten_json(message_dict)
    kafka_message_dict = {}
    filter_fields = [
        '_', 'id', 'date', 'message',
        'from_id_user_id', 'from_user', 'reply_to_reply_to_msg_id',
        'views', 'forwards', 'reactions', 'replies_replies'
    ]
    for field in filter_fields:
        kafka_message_dict[field] = message_flat.get(field, None)
    if kafka_message_dict['from_id_user_id']:
        user_name = await client.get_entity(kafka_message_dict['from_id_user_id'])
        kafka_message_dict['from_user'] = user_name.username
    return kafka_message_dict
