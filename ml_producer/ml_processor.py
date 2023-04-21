from kafka import KafkaConsumer, KafkaProducer
import json
from transformers import pipeline
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.ml_utils import process_message_ml

# Initialize sentiment analysis model
model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

# Initialize emotion classification model
classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", top_k=None)

kafka_topic_input = os.getenv('KAFKA_INPUT_TOPIC')
kafka_topic_output = os.getenv('KAFKA_OUTPUT_TOPIC')
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic_input,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False
)
print(f"Kafka consumer created for {kafka_topic_input} topic, {kafka_bootstrap_servers} bootstrap servers")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print(f"Kafka producer created for {kafka_topic_output} topic, {kafka_bootstrap_servers} bootstrap servers")

# Polling interval in milliseconds
poll_interval = 1000

# Infinite loop for processing messages
while True:
    # Poll for new messages
    min_length = 10
    messages = consumer.poll(poll_interval)
    # Process messages if available
    for tp, msgs in messages.items():
        for message in msgs:
            msg = message.value
            if msg['message'] is None or len(msg['message']) < min_length:
                continue
            processed_msg = process_message_ml(msg, sentiment_task, classifier)
            try:
                producer.send(kafka_topic_output, processed_msg)
                print(f"Message {processed_msg['id']} sent to Kafka")
            except Exception as e:
                print(f"Error sending message {processed_msg['id']} to Kafka: {e}")
