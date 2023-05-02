import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.ml_utils import process_ml_messages

if __name__ == '__main__':
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

    # Read environment variables for discord channel
    discord_input_topic = os.getenv('DISCORD_INPUT_TOPIC')
    discord_output_topic = os.getenv('DISCORD_ML_TOPIC')

    print(f"The discord input topic is {discord_input_topic}")
    process_ml_messages(discord_input_topic, discord_output_topic, kafka_bootstrap_servers, 'offset_discord.txt')
