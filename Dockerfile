FROM python:3.9-slim-buster

WORKDIR /app

COPY telegram_api telegram_api
COPY utils utils
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Start the producer
CMD ["python3", "-u", "telegram_api/telegram.py"]
