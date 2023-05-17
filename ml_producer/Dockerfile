FROM python:3.9-slim-buster

WORKDIR /app

COPY ml_producer/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt


COPY ml_producer ml_producer
COPY utils/common_utils.py utils/common_utils.py

CMD [ "python", "./ml_producer/ml_prod.py" ]
