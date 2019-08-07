FROM python:3.7.3-slim

RUN mkdir /app
COPY . /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt pytest

LABEL name=contessa version=test