FROM python:3.9-slim-buster

WORKDIR /app

COPY discord_producer discord_producer
COPY utils utils

# Install dependencies
RUN pip3 install --no-cache-dir -r discord_producer/requirements.txt

# Start the producer
CMD ["python3", "-u", "discord_producer/discord_producer.py"]
