from utils.common_utils import flatten_json


def process_message(message_dict):
    message_flat = flatten_json(message_dict)
    kafka_message_dict = {}
    filter_fields = [
        'id', 'type', 'content', 'author_id', 'author_username', 'timestamp',
        'referenced_message_author_id', 'referenced_message_author_username', 'referenced_message_id'
    ]
    for field in filter_fields:
        kafka_message_dict[field] = message_flat.get(field, None)
    kafka_message_dict['message'] = kafka_message_dict.pop('content', None)

    return kafka_message_dict
