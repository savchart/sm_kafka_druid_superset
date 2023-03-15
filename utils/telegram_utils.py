import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


def get_chat_name():
    return os.getenv("CHAT_NAME")


def get_chat_id():
    return int(os.getenv("CHAT_ID"))


def get_api_id():
    return int(os.getenv("API_ID"))


def get_api_hash():
    return os.getenv("API_HASH")


def get_bot_token():
    return os.getenv("BOT_TOKEN")


def get_username():
    return os.getenv("USERNAME")


def get_phone():
    return os.getenv("PHONE_NUMBER")
