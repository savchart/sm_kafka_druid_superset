# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ml_producer/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ml_producer ml_producer
COPY utils/common_utils.py utils/common_utils.py
COPY utils/ml_utils.py utils/ml_utils.py
COPY utils/model_utils.py utils/model_utils.py

# Run ml_telegram.py when the container launches
CMD ["./ml_producer/run.sh"]
