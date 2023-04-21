from utils.common_utils import flatten_json


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
