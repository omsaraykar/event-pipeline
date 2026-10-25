FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY event_factory.py producer.py ./
USER 10001
ENTRYPOINT ["python", "producer.py"]
