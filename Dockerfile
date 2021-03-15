FROM python:3.7.3-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt pytest

COPY . /app

LABEL name=contessa version=test