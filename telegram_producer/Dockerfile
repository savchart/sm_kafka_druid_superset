FROM python:3.9-slim-buster

WORKDIR /app

COPY telegram_producer/requirements.txt .

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

COPY telegram_producer telegram_producer
COPY utils utils

# Start the producer
CMD ["python3", "-u", "telegram_producer/telegram.py"]
