FROM python:3.11-slim

WORKDIR /app

COPY trades/requirements.txt .

COPY trades/ /app

COPY shared/ /app/shared

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]