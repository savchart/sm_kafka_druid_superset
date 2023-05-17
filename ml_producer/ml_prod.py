import os
import sys
from transformers import pipeline

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.common_utils import create_producer, create_consumer

# Initialize sentiment analysis model
model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
sentiment_task = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

# Initialize emotion classification model
classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", top_k=None)


def process_messages_ml(msgs_, sentiment_task_, classifier_):
    processed_msgs_ = []
    min_length = 10
    for msg in msgs_:
        if msg is None or msg['message'] is None or len(msg['message']) < min_length:
            continue
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
        processed_msgs_.append(msg)
    return processed_msgs_


if __name__ == '__main__':
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

    discord_input_topic = os.getenv('DISCORD_INPUT_TOPIC')
    discord_output_topic = os.getenv('DISCORD_ML_TOPIC')
    telegram_input_topic = os.getenv('TELEGRAM_INPUT_TOPIC')
    telegram_output_topic = os.getenv('TELEGRAM_ML_TOPIC')

    telegram_consumer = create_consumer(kafka_topic_=telegram_input_topic,
                                        kafka_bootstrap_servers_=kafka_bootstrap_servers, group_id='telegram_group')
    discord_consumer = create_consumer(kafka_topic_=discord_input_topic,
                                       kafka_bootstrap_servers_=kafka_bootstrap_servers, group_id='discord_group')
    producer = create_producer(kafka_bootstrap_servers_=kafka_bootstrap_servers)

    # Polling interval in milliseconds
    poll_interval = 1200

    # Infinite loop for processing messages
    while True:
        # Poll for new messages from both consumers
        telegram_messages = telegram_consumer.poll(poll_interval)
        discord_messages = discord_consumer.poll(poll_interval)

        # Process messages if available
        for messages in [telegram_messages, discord_messages]:
            for tp, msgs in messages.items():
                raw_msgs = [message.value for message in msgs]
                if len(raw_msgs) == 0:
                    continue
                try:
                    processed_msgs = process_messages_ml(raw_msgs, sentiment_task, classifier)
                except Exception as e:
                    print(f"Error processing messages: {e}")
                    continue

                for processed_msg in processed_msgs:
                    try:
                        if tp.topic == discord_input_topic:
                            producer.send(discord_output_topic, processed_msg)
                            print(f"Message {processed_msg['id']} sent to {discord_output_topic}")
                        else:
                            producer.send(telegram_output_topic, processed_msg)
                            print(f"Message {processed_msg['id']} sent to {telegram_output_topic}")
                    except Exception as e:
                        print(f"Error sending message {processed_msg['id']} to Kafka: {e}")

        # Commit the offsets for both consumers
        try:
            telegram_consumer.commit_async()
            discord_consumer.commit_async()
        except Exception as e:
            print(f"Error committing offsets: {e}")

        # Flush the producer
        producer.flush()
