FROM python:3.9-slim-buster

WORKDIR /app

COPY discord_producer/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY discord_producer discord_producer
COPY utils utils

# Install dependencies

# Start the producer
CMD ["python3", "-u", "discord_producer/discord_producer.py"]
