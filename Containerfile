FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV FLASK_APP=main.py

CMD ["gunicorn", "--bind", "0.0.0.0:9142", "main:app"]
