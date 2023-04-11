import os
import json
import discord
from kafka import KafkaProducer

import dotenv

from discord.ext import (
    commands,
)

dotenv.load_dotenv()

# Set up Kafka producer
# kafka_topic = os.getenv('KAFKA_DISCORD_INPUT_TOPIC')  # Get your Kafka topic from an env_druid variable
# kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
# producer = KafkaProducer(
#     bootstrap_servers=kafka_bootstrap_servers,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# Set up Discord client
TOKEN = os.getenv('DISCORD_TOKEN')  # Get your Discord bot token from an env_druid variable
SERVER_ID = os.getenv('DISCORD_SERVER_ID')  # Get your Discord channel ID from an env_druid variable
CHANNEL_ID = os.getenv('DISCORD_GENERAL_ID')  # Get your Discord channel ID from an env_druid variable

bot = commands.Bot(command_prefix='!')
bot.remove_command('help')
bot.run(TOKEN)