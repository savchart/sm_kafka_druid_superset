# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ml_producer ml_producer
COPY utils utils

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r ml_producer/requirements.txt

# Run ml_processor.py when the container launches
CMD ["python3", "-u", "ml_producer/ml_processor.py"]
