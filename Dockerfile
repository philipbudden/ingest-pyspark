FROM mcr.microsoft.com/devcontainers/python:3-bookworm

ENV PYSPARK_HADOOP_VERSION=3

RUN apt update && \
  apt install -y \
    curl \
    openjdk-17-jdk && \
  rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt
