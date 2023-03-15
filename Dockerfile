FROM python:3.9-slim-buster

WORKDIR /app

COPY telegram_api telegram_api
COPY utils utils
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set environment variables
ENV KAFKA_BROKER_URL=kafka:9092
ENV KAFKA_TOPIC=telegram-messages
ENV TELEGRAM_USERNAME=your_username
ENV TELEGRAM_PHONE_NUMBER=your_phone_number
ENV TELEGRAM_CHAT_NAME=your_chat_name
ENV TELEGRAM_CHAT_ID=your_chat_id
ENV TELEGRAM_API_ID=your_api_id
ENV TELEGRAM_API_HASH=your_api_hash
ENV TELEGRAM_BOT_TOKEN=your_bot_token

# Start the producer
CMD ["python3", "-u", "telegram_api/telegram.py"]
