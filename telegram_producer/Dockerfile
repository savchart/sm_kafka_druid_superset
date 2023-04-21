FROM python:3.9-slim-buster

WORKDIR /app

COPY telegram_producer telegram_producer
COPY utils utils

# Install dependencies
RUN pip3 install --no-cache-dir -r telegram_producer/requirements.txt

# Start the producer
CMD ["python3", "-u", "telegram_producer/telegram.py"]
