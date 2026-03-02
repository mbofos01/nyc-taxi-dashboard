FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# If not defined in docker-compose we default to 8000
ARG API_PORT=8000 
ENV API_PORT=${API_PORT}
EXPOSE ${API_PORT}
