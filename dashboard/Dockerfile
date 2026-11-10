FROM python:3.12-alpine
WORKDIR /app

RUN apk add --no-cache docker-cli docker-cli-compose && \
    pip install --no-cache-dir fastapi uvicorn websockets

COPY dashboard /app/dashboard

EXPOSE 8080
CMD ["python", "dashboard/server.py"]
