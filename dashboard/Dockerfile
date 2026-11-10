FROM python:3.12-alpine
WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn websockets

COPY dashboard /app/dashboard

EXPOSE 8080
CMD ["python", "dashboard/server.py"]
