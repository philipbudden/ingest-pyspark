FROM mcr.microsoft.com/devcontainers/python:3-bookworm

ENV PYSPARK_HADOOP_VERSION=3

RUN apt update && \
  apt install -y \
    curl \
    openjdk-17-jdk && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt && mkdir -p /logs

CMD ["python", "main.py"]
