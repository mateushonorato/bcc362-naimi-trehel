FROM python:3.12-alpine
WORKDIR /app
COPY observer.py node.py client.py ./
CMD ["python", "node.py"]