import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.ml_utils import process_ml_messages

if __name__ == '__main__':
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

    telegram_input_topic = os.getenv('TELEGRAM_INPUT_TOPIC')
    telegram_output_topic = os.getenv('TELEGRAM_ML_TOPIC')

    # Read environment variables for discord channel
    print(f"The telegram input topic is {telegram_input_topic}")
    process_ml_messages(telegram_input_topic, telegram_output_topic, kafka_bootstrap_servers)
