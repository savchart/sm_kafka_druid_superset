from transformers import pipeline
from utils.common_utils import create_producer, create_consumer
import os

# Initialize sentiment analysis model
model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

# Initialize emotion classification model
classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", top_k=None)

def process_message_ml(msg, sentiment_task_, classifier_):
    # Truncate the message if it's too long for the model
    max_length = 428  # This may vary depending on the model
    truncated_message = msg['message'][:max_length]

    # Process the truncated message with sentiment analysis and emotion classification models
    sentiment_result = sentiment_task_(truncated_message)
    emotion_result = classifier_(truncated_message)

    # Add results to the message dictionary
    msg['sentiment'] = sentiment_result[0]['label']
    msg['sentiment_score'] = sentiment_result[0]['score']

    # get emotion and score with max score
    max_emotion = max(emotion_result[0], key=lambda x: x['score'])
    msg['emotion'] = max_emotion['label']
    msg['emotion_score'] = max_emotion['score']

    return msg


def process_ml_messages(kafka_input_topic, kafka_output_topic, kafka_bootstrap_servers_, offset_file_path='offset.txt'):
    last_processed_offset = None
    if os.path.exists(offset_file_path):
        with open(offset_file_path, 'r') as f:
            last_processed_offset = int(f.read().strip())
    consumer = create_consumer(offset='earliest', kafka_topic_=kafka_input_topic,
                               kafka_bootstrap_servers_=kafka_bootstrap_servers_)
    producer = create_producer(kafka_topic_=kafka_output_topic, kafka_bootstrap_servers_=kafka_bootstrap_servers_)



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
                    producer.send(kafka_output_topic, processed_msg)
                    print(f"Message {processed_msg['id']} sent to {kafka_output_topic}")
                except Exception as e:
                    print(f"Error sending message {processed_msg['id']} to Kafka: {e}")
