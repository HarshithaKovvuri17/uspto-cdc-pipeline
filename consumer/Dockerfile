FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# Use unbuffered output to see logs in real-time inside Docker
CMD ["python", "-u", "consumer.py"]
