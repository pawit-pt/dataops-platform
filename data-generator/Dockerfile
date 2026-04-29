FROM python:3.11-slim

WORKDIR /app

RUN pip install kafka-python==2.0.2

COPY generator.py .

ENTRYPOINT ["python", "generator.py"]
