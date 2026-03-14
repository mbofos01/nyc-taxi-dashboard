# ETL Control API Service
FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG API_PORT=8000
ENV API_PORT=${API_PORT}
EXPOSE ${API_PORT}
